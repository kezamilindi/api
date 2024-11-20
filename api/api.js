// Load environment variables
require('dotenv').config();
const natural = require('natural');
const tokenizer = new natural.WordTokenizer();
const fs = require('fs').promises;
const admin = require('firebase-admin');


// Initialize Firebase Admin SDK
const credential = admin.credential.cert({
  type: "service_account",
  project_id: process.env.FIREBASE_PROJECT_ID,
  private_key_id: process.env.FIREBASE_PRIVATE_KEY_ID,
  private_key: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  client_email: process.env.FIREBASE_CLIENT_EMAIL,
  client_id: process.env.FIREBASE_CLIENT_ID,
  auth_uri: "https://accounts.google.com/o/oauth2/auth",
  token_uri: "https://oauth2.googleapis.com/token",
  auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
  client_x509_cert_url: process.env.FIREBASE_CLIENT_X509_CERT_URL
});

admin.initializeApp({
  credential: credential,
  databaseURL: process.env.FIREBASE_DATABASE_URL
});

// Constants
const INTERACTION_WEIGHTS = {
  liked: 0.3,
  reactions: 0.6,
  commented: 0.6,
  participate: 0.9,
  watched: 0.7
};

const MAX_VIDEO_DURATION = 90; // seconds

/**
 * Fetches user's viewing history with detailed information for each post
 * @param {string} userId - The user's ID
 * @param {string} contentType - Type of content ('video', 'text', or 'image')
 * @returns {Promise<Object>} Object containing categorized posts with details
 */
async function fetchUserViewingHistory(userId, contentType) {
  try {
    if (!['video', 'text', 'image'].includes(contentType)) {
      throw new Error('Invalid content type. Must be video, text, or image');
    }

    // Initialize categories with more detailed structure
    const categorizedHistory = {
      liked: {},
      none: {},
      participate: {},
      reactions: {},
      watched: {},
      commented: {}
    };

    const db = admin.database();
    const historyRef = db.ref(`/history/${userId}/${contentType}`);
    const historyData = await historyRef.once('value').then(snapshot => snapshot.val());

    if (!historyData) {
      return categorizedHistory;
    }

    // Process each category
    Object.entries(historyData).forEach(([category, posts]) => {
      if (!categorizedHistory.hasOwnProperty(category) || !posts) {
        return;
      }

      if (category === 'watched') {
        // Special handling for watched category to include additional data
        Object.entries(posts).forEach(([postId, postData]) => {
          categorizedHistory[category][postId] = {
            timestamp: postData?.timestamp || 0,
            watchTime: postData?.watchTime || 0,
            subpostsViewed: postData?.subpostsViewed || [],
            subpostsWatchTime: postData?.subpostsWatchTime || {}
          };
        });
      } else {
        // For other categories (including commented), just get timestamp
        Object.entries(posts).forEach(([postId, postData]) => {
          const timestamp = typeof postData === 'number' ? postData : postData?.timestamp || 0;
          categorizedHistory[category][postId] = {
            timestamp: timestamp
          };
        });
      }
    });

    return categorizedHistory;

  } catch (error) {
    console.error('Error fetching user viewing history:', error);
    throw error;
  }
}

/**
 * Fetch post upload timestamp from content_type_challenges
 * @param {string} contentType - The type of content
 * @param {string} postId - The post ID
 * @returns {Promise<number>} The timestamp or 0
 */
async function fetchPostTimestamp(contentType, postId) {
    try {
      const ref = admin.database().ref(`/${contentType}_challenges/${postId}/timestamp`);
      const timestamp = await ref.once('value').then(snapshot => snapshot.val());
      return timestamp || 0;
    } catch (error) {
      console.error('Error fetching post timestamp:', error);
      return 0;
    }
  }
  
  /**
   * Calculate interaction score for a single post
   * @param {string} postId - The post ID
   * @param {Object} interactions - The interactions object
   * @returns {Promise<number>} The calculated score
   */
  async function calculatePostInteractionScore(postId, interactions) {
    let score = 0.0;
    let interactionCount = 0;
  
    for (const [category, weight] of Object.entries(INTERACTION_WEIGHTS)) {
      if (category in interactions && postId in interactions[category]) {
        if (category === 'watched') {
          const watchTime = interactions[category][postId]?.watchTime || 0;
          // Normalize watch time against max duration
          const watchScore = (watchTime / MAX_VIDEO_DURATION) * weight;
          score += Math.min(watchScore, weight); // Cap at maximum weight
        } else {
          score += weight;
        }
        interactionCount++;
      }
    }
  
    return interactionCount > 0 ? score / Object.keys(INTERACTION_WEIGHTS).length : 0.0;
  }
  
  /**
   * Calculate user engagement metrics including interaction scores and rates
   * @param {Object} history - The user's interaction history
   * @returns {Promise<Object>} The engagement metrics
   */
  async function calculateUserEngagementMetrics(history) {
    try {
      // Get unique post IDs across all categories
      const allPosts = new Set();
      Object.values(history).forEach(category => {
        Object.keys(category).forEach(postId => allPosts.add(postId));
      });
  
      // Calculate interaction scores for each post
      const postScores = {};
      await Promise.all(Array.from(allPosts).map(async postId => {
        postScores[postId] = await calculatePostInteractionScore(postId, history);
      }));
  
      // Calculate engagement rates
      const totalPosts = allPosts.size;
      const engagementRates = {
        like_rate: totalPosts > 0 ? Object.keys(history.liked || {}).length / totalPosts : 0,
        reaction_rate: totalPosts > 0 ? Object.keys(history.reactions || {}).length / totalPosts : 0,
        comment_rate: totalPosts > 0 ? Object.keys(history.commented || {}).length / totalPosts : 0,
        participate_rate: totalPosts > 0 ? Object.keys(history.participate || {}).length / totalPosts : 0,
        watch_rate: totalPosts > 0 ? Object.keys(history.watched || {}).length / totalPosts : 0,
      };
  
      // Calculate average watch completion rate
      const watchedPosts = history.watched || {};
      if (Object.keys(watchedPosts).length > 0) {
        const totalCompletion = Object.values(watchedPosts).reduce((sum, postData) => 
          sum + Math.min((postData?.watchTime || 0) / MAX_VIDEO_DURATION, 1.0), 0);
        engagementRates.avg_watch_completion = totalCompletion / Object.keys(watchedPosts).length;
      } else {
        engagementRates.avg_watch_completion = 0;
      }
  
      // Calculate average interaction score
      const avgInteractionScore = Object.values(postScores).length > 0 
        ? Object.values(postScores).reduce((a, b) => a + b, 0) / Object.values(postScores).length 
        : 0;
  
      return {
        post_scores: postScores,
        engagement_rates: engagementRates,
        avg_interaction_score: avgInteractionScore
      };
  
    } catch (error) {
      console.error('Error calculating engagement metrics:', error);
      throw error;
    }
  }
  
  /**
   * Calculate adaptive decay rates based on user interaction patterns
   * @param {Object} history - The user's interaction history
   * @param {string} contentType - The type of content
   * @returns {Promise<Object>} The decay metrics
   */
  async function calculateDecayMetrics(history, contentType) {
    try {
      // Constants for time windows (in seconds)
      const NEW_CONTENT_THRESHOLD = 24 * 60 * 60; // 24 hours
      const OLD_CONTENT_THRESHOLD = 7 * 24 * 60 * 60; // 1 week
  
      // Initialize decay metrics
      const decayMetrics = {
        liked_decay: [],
        reactions_decay: [],
        commented_decay: [],
        watched_decay: [],
        participate_decay: []
      };
  
      // Process interactions
      const processInteraction = async (category, postId, interactionData) => {
        try {
          const interactionTime = interactionData?.timestamp || 0;
          if (!interactionTime) return null;
  
          const postTime = await fetchPostTimestamp(contentType, postId);
          if (!postTime) return null;
  
          const timeDiff = interactionTime - postTime;
  
          // Calculate decay factor
          let decay;
          if (timeDiff <= NEW_CONTENT_THRESHOLD) {
            decay = 1.0;
          } else if (timeDiff >= OLD_CONTENT_THRESHOLD) {
            decay = 0.3;
          } else {
            decay = 0.3 + (0.7 * (OLD_CONTENT_THRESHOLD - timeDiff) / 
                          (OLD_CONTENT_THRESHOLD - NEW_CONTENT_THRESHOLD));
          }
  
          return { category, decay, timeDiff };
        } catch (error) {
          console.error('Error processing interaction:', error);
          return null;
        }
      };
  
      // Process all interactions concurrently
      const tasks = [];
      for (const [category, posts] of Object.entries(history)) {
        if (category === 'none') continue;
        for (const [postId, interactionData] of Object.entries(posts)) {
          tasks.push(processInteraction(category, postId, interactionData));
        }
      }
  
      const results = (await Promise.all(tasks)).filter(r => r !== null);
  
      // Calculate adaptive decay rates
      results.forEach(result => {
        const decayKey = `${result.category}_decay`;
        if (decayKey in decayMetrics) {
          decayMetrics[decayKey].push(result.decay);
        }
      });
  
      // Calculate final decay rates
      const finalDecayRates = {};
      const currentTime = Date.now() / 1000;
  
      for (const [category, decays] of Object.entries(decayMetrics)) {
        if (decays.length > 0) {
          const avgDecay = decays.reduce((a, b) => a + b, 0) / decays.length;
          const recentInteractions = decays.filter(d => d > 0.7).length;
          const frequencyFactor = decays.length > 0 ? recentInteractions / decays.length : 0;
          const adjustedDecay = avgDecay * (1 + frequencyFactor);
          finalDecayRates[category] = Math.min(Math.max(adjustedDecay, 0.1), 1.0);
        } else {
          finalDecayRates[category] = 0.5; // Default decay rate
        }
      }
  
      return finalDecayRates;
  
    } catch (error) {
      console.error('Error calculating decay metrics:', error);
      throw error;
    }
  }

  /**
 * Calculate user engagement metrics including interaction scores and decay rates
 * @param {Object} history - The user's interaction history
 * @param {string} contentType - The type of content
 * @returns {Promise<Object>} The engagement metrics
 */
async function calculateUserEngagementMetrics(history, contentType) {
    try {
      // Get unique post IDs across all categories
      const allPosts = new Set();
      Object.values(history).forEach(category => {
        Object.keys(category).forEach(postId => allPosts.add(postId));
      });
  
      // Calculate interaction scores for each post
      const postScores = {};
      await Promise.all(Array.from(allPosts).map(async postId => {
        postScores[postId] = await calculatePostInteractionScore(postId, history);
      }));
  
      // Calculate engagement rates
      const totalPosts = allPosts.size;
      const engagementRates = {
        like_rate: totalPosts > 0 ? Object.keys(history.liked || {}).length / totalPosts : 0,
        reaction_rate: totalPosts > 0 ? Object.keys(history.reactions || {}).length / totalPosts : 0,
        comment_rate: totalPosts > 0 ? Object.keys(history.commented || {}).length / totalPosts : 0,
        participate_rate: totalPosts > 0 ? Object.keys(history.participate || {}).length / totalPosts : 0,
        watch_rate: totalPosts > 0 ? Object.keys(history.watched || {}).length / totalPosts : 0,
      };
  
      // Calculate average watch completion rate
      const watchedPosts = history.watched || {};
      if (Object.keys(watchedPosts).length > 0) {
        const totalCompletion = Object.values(watchedPosts).reduce((sum, postData) => 
          sum + Math.min((postData?.watchTime || 0) / MAX_VIDEO_DURATION, 1.0), 0);
        engagementRates.avg_watch_completion = totalCompletion / Object.keys(watchedPosts).length;
      } else {
        engagementRates.avg_watch_completion = 0;
      }
  
      // Calculate average interaction score
      const avgInteractionScore = Object.values(postScores).length > 0 
        ? Object.values(postScores).reduce((a, b) => a + b, 0) / Object.values(postScores).length 
        : 0;
  
      // Calculate decay rates
      const decayRates = await calculateDecayMetrics(history, contentType);
  
      return {
        post_scores: postScores,
        engagement_rates: engagementRates,
        avg_interaction_score: avgInteractionScore,
        decay_rates: decayRates
      };
  
    } catch (error) {
      console.error('Error calculating engagement metrics:', error);
      throw error;
    }
  }
  

 /**
 * Fetch and process subposts for a given post
 * @param {string} postId - The parent post ID
 * @returns {Promise<Object>} Subpost data and metrics
 */
 async function fetchSubpostsData(postId) {
  try {
    // Correct path for subposts
    const subpostsRef = admin.database().ref(`/subposts/${postId}`);
    const subpostsSnapshot = await subpostsRef.once('value');
    const subposts = subpostsSnapshot.val();

    if (!subposts) {
      return {
        count: 0,
        creator_ids: [],
        avg_metrics: {
          reaction_count: 0,
          watch_count: 0,
          watch_time: 0,
          comment_count: 0
        },
        vectors: {
          engagement: {
            reaction_rate: 0,
            watch_completion_rate: 0,
            comment_rate: 0
          },
          popularity: {
            reaction_velocity: 0,
            watch_velocity: 0,
            comment_velocity: 0
          }
        }
      };
    }

    const subpostEntries = Object.entries(subposts);
    const currentTime = Math.floor(Date.now() / 1000);
    const creator_ids = new Set();
    let totalReactions = 0;
    let totalWatchCount = 0;
    let totalWatchTime = 0;
    let totalComments = 0;
    let totalViews = 0;

    // Process each subpost
    await Promise.all(subpostEntries.map(async ([subpostId, subpostData]) => {
      // Correct path for fetching full subpost data
      const subpostRef = admin.database().ref(`/subposts/${postId}/${subpostId}`);
      const subpostSnapshot = await subpostRef.once('value');
      const fullSubpostData = subpostSnapshot.val() || {};

      const {
        creator_id = 'unknown',
        reaction_count = 0,
        watch_count = 0,
        watch_time = 0,
        comment_count = 0,
        views = 0,
        timestamp = currentTime
      } = fullSubpostData;

      if (creator_id !== 'unknown') {
        creator_ids.add(creator_id);
      }
      
      totalReactions += reaction_count;
      totalWatchCount += watch_count;
      totalWatchTime += watch_time;
      totalComments += comment_count;
      totalViews += views || watch_count; // fallback to watch_count if views not available
    }));

    const subpostCount = subpostEntries.length;
    const avgViews = Math.max(totalViews / subpostCount, 1);
    const timestamp = subposts[Object.keys(subposts)[0]]?.timestamp || currentTime;
    const timeDiff = Math.max(currentTime - timestamp, 1);

    // Calculate average metrics
    const avgMetrics = {
      reaction_count: totalReactions / subpostCount,
      watch_count: totalWatchCount / subpostCount,
      watch_time: totalWatchTime / subpostCount,
      comment_count: totalComments / subpostCount
    };

   // Normalize engagement metrics (0 to 1 range)
   const engagementVector = {
    reaction_rate: Math.min(totalReactions / (avgViews * subpostCount), 1.0),
    watch_completion_rate: Math.min(totalWatchTime / (avgViews * subpostCount * MAX_VIDEO_DURATION), 1.0),
    comment_rate: Math.min(totalComments / (avgViews * subpostCount), 1.0)
  };

// Calculate interaction score for subposts
const subpostInteractionScore = (
  (engagementVector.reaction_rate * 0.6) +
  (engagementVector.watch_completion_rate * 0.7) +
  (engagementVector.comment_rate * 0.6)
) / 3; // Normalize by number of metrics

   
    // Normalize popularity metrics (velocity relative to time difference)
    const popularityVector = {
      interaction_velocity: subpostInteractionScore / timeDiff,
      reaction_velocity: (engagementVector.reaction_rate * 0.6) / timeDiff,
      watch_velocity: (engagementVector.watch_completion_rate * 0.7) / timeDiff,
      comment_velocity: (engagementVector.comment_rate * 0.6) / timeDiff
    };

    return {
      count: subpostCount,
      creator_ids: Array.from(creator_ids),
      avg_metrics: avgMetrics,
      vectors: {
        engagement: engagementVector,
        popularity: popularityVector,
        interaction_score: subpostInteractionScore
      }
    };

  } catch (error) {
    console.error(`Error processing subposts for post ${postId}:`, error);
    throw error;
  }
}

/**
 * Process a batch of posts
 * @param {Array} postBatch - Array of [postId, postData] entries
 * @param {number} currentTime - Current timestamp
 * @param {string} contentType - Type of content ('video', 'text', or 'image')
 * @returns {Promise<Object>} Processed post vectors
 */
async function processPostBatch(postBatch, currentTime, contentType) { // Added contentType parameter
  if (!contentType) {
    throw new Error('Content type must be specified');
  }
  const batchVectors = {};
  
  await Promise.all(postBatch.map(async ([postId, postData]) => {
    try {
      // Use contentType parameter for the correct path
      const postRef = admin.database().ref(`/${contentType}_challenges/${postId}`);
      const postSnapshot = await postRef.once('value');
      const fullPostData = postSnapshot.val() || {};

      // Fetch subposts data
      const subpostsData = await fetchSubpostsData(postId);

      // Rest of your existing post processing code...
      const {
        creator_id = 'unknown',
        description = '',
        timestamp = currentTime,
        comment_count = 0,
        likes_count = 0,
        participate_count = 0,
        reaction_count = 0,
        views = 0,
        watch_count = 0,
        watch_time = 0
      } = fullPostData;

      // Your existing vector calculations...
      const safeViews = Math.max(views, 1);
      const timeDiff = Math.max(currentTime - timestamp, 1);

      const engagementVector = {
        participation_rate: participate_count / safeViews,
        reaction_rate: reaction_count / safeViews,
        like_rate: likes_count / safeViews,
        comment_rate: comment_count / safeViews,
        watch_completion_rate: watch_time / (safeViews * MAX_VIDEO_DURATION)
      };

      const interactionScore = (
        (participate_count * 0.9) +
        (reaction_count * 0.6) +
        (likes_count * 0.3) +
        (comment_count * 0.6) +
        (Math.min(watch_time / MAX_VIDEO_DURATION, 1) * 0.7)
      ) / 5;

      const popularityVector = {
        interaction_velocity: interactionScore / timeDiff,
        participation_velocity: (participate_count * 0.9) / timeDiff,
        like_velocity: (likes_count * 0.3) / timeDiff,
        comment_velocity: (comment_count * 0.6) / timeDiff,
        reaction_velocity: (reaction_count * 0.6) / timeDiff,
        watch_velocity: (Math.min(watch_time / MAX_VIDEO_DURATION, 1) * 0.7) / timeDiff
      };

      batchVectors[postId] = {
        metadata: {
          creator_id,
          description,
          timestamp,
          total_views: views,
          age_seconds: timeDiff
        },
        metrics: {
          comment_count,
          likes_count,
          participate_count,
          reaction_count,
          views,
          watch_count,
          watch_time,
          interaction_score: interactionScore
        },
        vectors: {
          engagement: engagementVector,
          popularity: popularityVector
        },
        subposts: subpostsData // Add subposts data to the response
      };
    } catch (error) {
      console.error(`Error processing post ${postId}:`, error);
    }
  }));

  return batchVectors;
}

/**
 * Fetch and calculate post vectors for all posts in content_type_challenges
 * @param {string} contentType - The type of content ('video', 'text', or 'image')
 * @returns {Promise<Object>} Object containing post vectors and metadata
 */
async function fetchPostVectors(contentType) {
  try {
    if (!contentType) {
      throw new Error('Content type must be specified');
    }

    const db = admin.database();
    const postsRef = db.ref(`/${contentType}_challenges`);
    const currentTime = Math.floor(Date.now() / 1000);
    
    // Fetch all posts
    const postsSnapshot = await postsRef.once('value');
    const posts = postsSnapshot.val();
    
    if (!posts) {
      return {};
    }

    const postEntries = Object.entries(posts);
    const batchSize = 5;
    const postVectors = {};
    
    // Process first 5 batches concurrently (25 posts)
    const initialBatches = [];
    for (let i = 0; i < 5 && i * batchSize < postEntries.length; i++) {
      const start = i * batchSize;
      const batch = postEntries.slice(start, start + batchSize);
      // Pass contentType here
      initialBatches.push(processPostBatch(batch, currentTime, contentType));
    }

    // Wait for initial batches to complete
    const initialResults = await Promise.all(initialBatches);
    initialResults.forEach(batchResult => {
      Object.assign(postVectors, batchResult);
    });

    // Process remaining posts in batches
    for (let i = 5 * batchSize; i < postEntries.length; i += batchSize) {
      const batch = postEntries.slice(i, i + batchSize);
      // Pass contentType here
      const batchResult = await processPostBatch(batch, currentTime, contentType);
      Object.assign(postVectors, batchResult);
    }

    return postVectors;

  } catch (error) {
    console.error('Error calculating post vectors:', error);
    throw error;
  }
}

/**
 * Filter posts based on user's viewing history
 * @param {Object} history - The user's viewing history
 * @param {Object} allPosts - All posts from the database
 * @returns {Object} An object containing posts in history and not in history
 */
function filterPostsByHistory(history, allPosts) {
  const postsInHistory = {};
  const postsNotInHistory = {};

  // Flatten the history to get all post IDs the user has interacted with
  const historyPostIds = new Set();
  Object.values(history).forEach(category => {
    Object.keys(category).forEach(postId => historyPostIds.add(postId));
  });

  // Separate posts into those in history and those not in history
  Object.entries(allPosts).forEach(([postId, postData]) => {
    if (historyPostIds.has(postId)) {
      postsInHistory[postId] = postData;
    } else {
      postsNotInHistory[postId] = postData;
    }
  });

  return {
    postsInHistory,
    postsNotInHistory
  };
}

{
  calculateUserEngagementMetrics,
  fetchPostVectors,  // Add this line
  main
};

/**
 * Analyze creator frequencies in user's viewing history - Optimized Version
 * @param {string} userId - The user's ID
 * @param {string} contentType - Type of content
 * @returns {Promise<Object>} Creator frequency analysis
 */
async function analyzeCreatorFrequency(userId, contentType) {
  try {
    // Cache key for this specific analysis
    const cacheKey = `creator_frequency_${userId}_${contentType}`;
    const cacheFile = `./cache/${cacheKey}.json`;

    // Try to read from cache first
    try {
      const cached = await fs.readFile(cacheFile, 'utf8');
      return JSON.parse(cached);
    } catch (e) {
      // Cache miss or error, proceed with analysis
    }

    // Get user's viewing history and initialize frequency tracking
    const [history, db] = await Promise.all([
      fetchUserViewingHistory(userId, contentType),
      admin.database()
    ]);

    const creatorFrequency = new Map();
    const historyPostIds = new Set();
    
    // Collect all post IDs (optimized from previous version)
    for (const category of Object.values(history)) {
      for (const postId of Object.keys(category)) {
        historyPostIds.add(postId);
      }
    }

    // Prepare batch references for main posts and subposts
    const postRefs = Array.from(historyPostIds).map(postId => ({
      main: db.ref(`/${contentType}_challenges/${postId}`),
      sub: db.ref(`/subposts/${postId}`)
    }));

    // Fetch all data in parallel
    const results = await Promise.all(
      postRefs.map(async refs => {
        try {
          const [mainPost, subposts] = await Promise.all([
            refs.main.once('value'),
            refs.sub.once('value')
          ]);

          return {
            mainPost: mainPost.val(),
            subposts: subposts.val()
          };
        } catch (error) {
          console.warn('Error fetching post data:', error);
          return null;
        }
      })
    );

    // Process results (now in memory)
    results.forEach(result => {
      if (!result) return;

      // Process main post
      if (result.mainPost?.creator_id) {
        creatorFrequency.set(
          result.mainPost.creator_id,
          (creatorFrequency.get(result.mainPost.creator_id) || 0) + 1
        );
      }

      // Process subposts
      if (result.subposts) {
        Object.values(result.subposts).forEach(subpost => {
          if (subpost?.creator_id) {
            creatorFrequency.set(
              subpost.creator_id,
              (creatorFrequency.get(subpost.creator_id) || 0) + 1
            );
          }
        });
      }
    });

    // Filter and sort creators (only those with frequency >= 2)
    const frequentCreators = Array.from(creatorFrequency.entries())
      .filter(([_, freq]) => freq >= 2)
      .sort((a, b) => b[1] - a[1]);

    // Calculate statistics
    const stats = {
      total_creators: creatorFrequency.size,
      frequent_creators: frequentCreators.length,
      max_frequency: frequentCreators[0]?.[1] || 0,
      avg_frequency: frequentCreators.length > 0
        ? frequentCreators.reduce((sum, [_, freq]) => sum + freq, 0) / frequentCreators.length
        : 0
    };

    const result = {
      stats,
      creators: Object.fromEntries(frequentCreators),
      timestamp: Date.now()
    };

    // Cache results asynchronously (don't await)
    fs.mkdir('./cache', { recursive: true })
      .then(() => fs.writeFile(cacheFile, JSON.stringify(result)))
      .catch(err => console.warn('Cache write failed:', err));

    return result;

  } catch (error) {
    console.error('Error in creator frequency analysis:', error);
    throw error;
  }
}

class ContentAnalyzer {
  constructor() {
      // Category keywords with associated scores
      this.categories = {
          sports: new Set(['game', 'play', 'team', 'win', 'sport', 'match', 'player', 'score', 'competition', 'athletic']),
          entertainment: new Set(['music', 'dance', 'sing', 'movie', 'show', 'performance', 'artist', 'concert', 'talent']),
          comedy: new Set(['funny', 'laugh', 'sometimes','joke', 'humor', 'comedy', 'hilarious', 'prank', 'parody', 'skit', '😂', 'woah', ' aura']),
          politics: new Set(['political', 'government', 'policy', 'election', 'vote', 'leader', 'debate', 'campaign']),
          motivation: new Set(['Quote','inspire', 'motivate', 'success', 'goal', 'dream', 'achieve', 'believe', 'positive'])
      };

      // Sentiment dictionary optimized for content analysis
      this.sentiments = {
          positive: new Set(['Nice', 'harder','approve','Dad-daughter','amazing', 'awesome', 'excellent', 'great', 'love', 'perfect', 'best', 'moments🌞']),
          negative: new Set(['bad', 'poor', 'terrible', 'worst', 'hate', 'boring', 'waste', 'Sad', 'stop', '😔','loose']),
          neutral: new Set(['okay', 'average', 'normal', 'standard', 'typical','valid'])
      };

      this.stemmer = natural.PorterStemmer;
  }

   /**
     * Analyze content from user viewing history
     * @param {Object} history - User viewing history
     * @param {string} contentType - Content type
     * @returns {Promise<Object>} Analysis results
     */
   async analyzeUserContent(history, contentType) {
    const startTime = Date.now();
    const descriptions = new Set();
    const categoryScores = {
        sports: 0,
        entertainment: 0,
        comedy: 0,
        politics: 0,
        motivation: 0
    };
    const sentimentScores = {
        positive: 0,
        negative: 0,
        neutral: 0
    };
   
        // Batch process posts
        const posts = await this.fetchPostDescriptions(history, contentType);
        
        // Process all descriptions
        posts.forEach(post => {
            if (!post.description) return;
            
            const words = this.preprocessText(post.description);
            descriptions.add(post.description);

            // Analyze categories and sentiments in single pass
            words.forEach(word => {
                const stemmed = this.stemmer.stem(word);
                
                // Check categories
                for (const [category, keywords] of Object.entries(this.categories)) {
                    if (keywords.has(stemmed)) {
                        categoryScores[category]++;
                    }
                }

                // Check sentiments
                for (const [sentiment, keywords] of Object.entries(this.sentiments)) {
                    if (keywords.has(stemmed)) {
                        sentimentScores[sentiment]++;
                    }
                }
            });
        });

           // Normalize scores
           const totalPosts = descriptions.size || 1;
           for (const category in categoryScores) {
               categoryScores[category] = categoryScores[category] / totalPosts;
           }
           for (const sentiment in sentimentScores) {
               sentimentScores[sentiment] = sentimentScores[sentiment] / totalPosts;
           }
   
           // Find dominant categories (top 2)
           const dominantCategories = Object.entries(categoryScores)
               .sort(([,a], [,b]) => b - a)
               .slice(0, 2)
               .filter(([,score]) => score > 0)
               .map(([category]) => category);
   
           const executionTime = Date.now() - startTime;

            
        return {
          analysis_time_ms: executionTime,
          total_posts: totalPosts,
          dominant_categories: dominantCategories,
          category_scores: categoryScores,
          sentiment_distribution: sentimentScores,
          content_summary: {
              primary_category: dominantCategories[0] || 'uncategorized',
              overall_sentiment: this.getOverallSentiment(sentimentScores)
          }
      };
  }

  /**
   * Fetch post descriptions efficiently
   * @private
   */
  async fetchPostDescriptions(history, contentType) {
      const postIds = new Set();
      Object.values(history).forEach(category => {
          Object.keys(category).forEach(postId => postIds.add(postId));
      });
      const db = admin.database();
      const posts = await Promise.all(
          Array.from(postIds).map(postId =>
              db.ref(`/${contentType}_challenges/${postId}/description`)
                  .once('value')
                  .then(snapshot => ({
                      id: postId,
                      description: snapshot.val()
                  }))
          )
      );

      return posts.filter(post => post.description);
  }

  /**
     * Preprocess text for analysis
     * @private
     */
  preprocessText(text) {
    return text.toLowerCase()
        .replace(/[^\w\s]/g, '')
        .split(/\s+/)
        .filter(word => word.length > 2);
}

/**
 * Calculate overall sentiment
 * @private
 */
getOverallSentiment(scores) {
    if (scores.positive > scores.negative && scores.positive > scores.neutral) {
        return 'positive';
    }
    if (scores.negative > scores.positive && scores.negative > scores.neutral) {
        return 'negative';
    }
    return 'neutral';

  }
}

/**
 * Cache and retrieve content summary for a user
 * @param {string} userId - The user's ID
 * @param {string} contentType - Type of content
 * @param {Object} summary - Content summary to cache (optional)
 * @returns {Promise<Object|null>} Cached content summary or null
 */
async function cacheContentSummary(userId, contentType, summary = null) {
  try {
    const cacheDir = './cache';
    const cacheFile = `${cacheDir}/content_summary_${userId}_${contentType}.json`;

    // If summary is provided, write to cache
    if (summary) {
      const cacheData = {
        summary,
        timestamp: Date.now(),
        userId,
        contentType
      };

      await fs.mkdir(cacheDir, { recursive: true });
      await fs.writeFile(cacheFile, JSON.stringify(cacheData));
      return cacheData;
    }

    // Try to read from cache
    try {
      const cached = await fs.readFile(cacheFile, 'utf8');
      const cacheData = JSON.parse(cached);
      
      // Check if cache is still valid (24 hours)
      const cacheAge = Date.now() - cacheData.timestamp;
      if (cacheAge < 24 * 60 * 60 * 1000) {
        return cacheData;
      }
    } catch (e) {
      // Cache miss or invalid cache
      return null;
    }

  } catch (error) {
    console.warn('Cache operation failed:', error);
    return null;
  }
}

/**
 * Get or analyze content summary for a user
 * @param {string} userId - The user's ID
 * @param {string} contentType - Type of content
 * @returns {Promise<Object>} Content summary
 */
async function getContentSummary(userId, contentType) {
  try {
    // Try to get cached summary
    const cached = await cacheContentSummary(userId, contentType);
    if (cached) {
      console.log('Retrieved from cache');
      return cached.summary;
    }

    // If no cache, perform analysis
    const history = await fetchUserViewingHistory(userId, contentType);
    const analyzer = new ContentAnalyzer();
    const analysis = await analyzer.analyzeUserContent(history, contentType);
    
    // Cache the new summary
    await cacheContentSummary(userId, contentType, analysis.content_summary);
    
    return analysis.content_summary;
  } catch (error) {
    console.error('Error getting content summary:', error);
    throw error;
  }
}

/**
 * Filter posts based on user's viewing history
 * @param {Object} history - The user's viewing history
 * @param {Object} allPosts - All posts from the database
 * @returns {Object} An object containing posts in history and not in history
 */
function filterPostsByHistory(history, allPosts) {
  const postsInHistory = {};
  const postsNotInHistory = {};

  // Flatten the history to get all post IDs the user has interacted with
  const historyPostIds = new Set();
  Object.values(history).forEach(category => {
    Object.keys(category).forEach(postId => historyPostIds.add(postId));
  });

  // Separate posts into those in history and those not in history
  Object.entries(allPosts).forEach(([postId, postData]) => {
    if (historyPostIds.has(postId)) {
      postsInHistory[postId] = postData;
    } else {
      postsNotInHistory[postId] = postData;
    }
  });

  return {
    postsInHistory,
    postsNotInHistory
  };
}

/**
 * Find similar posts based on creator frequency and content analysis
 * @param {string} userId - The user's ID
 * @param {string} contentType - Type of content
 * @returns {Promise<Object>} Similar posts with IDs and details
 */
async function findSimilarPosts(userId, contentType) {
  try {
    // Get user's preferences and data
    const [history, creatorAnalysis, contentSummary] = await Promise.all([
      fetchUserViewingHistory(userId, contentType),
      analyzeCreatorFrequency(userId, contentType),
      getContentSummary(userId, contentType)
    ]);

    const db = admin.database();
    const allPostsRef = db.ref(`/${contentType}_challenges`);
    const allPosts = await allPostsRef.once('value').then(snapshot => snapshot.val() || {});
    const { postsNotInHistory } = filterPostsByHistory(history, allPosts);

    // Initialize analyzer
    const analyzer = new ContentAnalyzer();
    const similarPosts = {
      by_creator: [],
      by_category: [],
      by_sentiment: []
    };

    // Process posts by creator first
    const postsByCreator = new Map();
    Object.entries(postsNotInHistory).forEach(([postId, postData]) => {
      if (creatorAnalysis.creators?.[postData.creator_id]) {
        similarPosts.by_creator.push({
          id: postId,
          ...postData,
          frequency: creatorAnalysis.creators[postData.creator_id]
        });
      }
    });

    // Analyze remaining posts for category and sentiment matches
    const remainingPosts = Object.entries(postsNotInHistory)
      .filter(([postId]) => !similarPosts.by_creator.find(p => p.id === postId));

    // Process posts in smaller batches
    const batchSize = 10;
    for (let i = 0; i < remainingPosts.length; i += batchSize) {
      const batch = remainingPosts.slice(i, i + batchSize);
      
      await Promise.all(batch.map(async ([postId, post]) => {
        if (!post.description) return;

        // Analyze post description
        const words = analyzer.preprocessText(post.description);
        const categoryScores = {
          sports: 0,
          entertainment: 0,
          comedy: 0,
          politics: 0,
          motivation: 0
        };
        const sentimentScores = {
          positive: 0,
          negative: 0,
          neutral: 0
        };

        // Count category and sentiment matches
        words.forEach(word => {
          const stemmed = analyzer.stemmer.stem(word);
          
          // Check categories
          for (const [category, keywords] of Object.entries(analyzer.categories)) {
            if (keywords.has(word) || keywords.has(stemmed)) {
              categoryScores[category]++;
            }
          }

          // Check sentiments
          for (const [sentiment, keywords] of Object.entries(analyzer.sentiments)) {
            if (keywords.has(word) || keywords.has(stemmed)) {
              sentimentScores[sentiment]++;
            }
          }
        });

        // Determine primary category and sentiment
        const primaryCategory = Object.entries(categoryScores)
          .reduce((a, b) => b[1] > a[1] ? b : a, ['uncategorized', 0])[0];

        const overallSentiment = Object.entries(sentimentScores)
          .reduce((a, b) => b[1] > a[1] ? b : a, ['neutral', 0])[0];

        // Add to appropriate categories if matching
        if (primaryCategory === contentSummary.primary_category) {
          similarPosts.by_category.push({
            id: postId,
            ...post,
            category: primaryCategory,
            category_score: categoryScores[primaryCategory]
          });
        }

        if (overallSentiment === contentSummary.overall_sentiment) {
          similarPosts.by_sentiment.push({
            id: postId,
            ...post,
            sentiment: overallSentiment,
            sentiment_score: sentimentScores[overallSentiment]
          });
        }
      }));
    }

    // Sort results by relevance scores
    similarPosts.by_category.sort((a, b) => b.category_score - a.category_score);
    similarPosts.by_sentiment.sort((a, b) => b.sentiment_score - a.sentiment_score);

    return {
      user_preferences: contentSummary,
      total_found: {
        by_creator: similarPosts.by_creator.length,
        by_category: similarPosts.by_category.length,
        by_sentiment: similarPosts.by_sentiment.length
      },
      similar_posts: similarPosts
    };

  } catch (error) {
    console.error('Error finding similar posts:', error);
    throw error;
  }
}


/**
 * Generate personalized recommendations using Thompson Sampling
 * @param {string} userId - User ID
 * @param {string} contentType - Content type
 * @param {number} numRecommendations - Number of recommendations to generate
 * @returns {Promise<Array>} Recommended posts
 */
async function generatePersonalizedRecommendations(userId, contentType, numRecommendations = 10) {
  try {
    // Fetch user history and engagement metrics
    const history = await fetchUserViewingHistory(userId, contentType);
    const userMetrics = await calculateUserEngagementMetrics(history, contentType);
    const decayRates = await calculateDecayMetrics(history, contentType);
    
    // Fetch all available posts
    const allPosts = await fetchPostVectors(contentType);
    
    // Helper function for beta distribution sampling
    function sampleBeta(successes, failures) {
      const alpha = 1 + successes;
      const beta = 1 + failures;
      const u = Math.random();
      const v = Math.random();
      return Math.pow(u, 1/alpha) / (Math.pow(u, 1/alpha) + Math.pow(v, 1/beta));
    }

    // Calculate engagement probabilities for each metric
    const engagementRates = userMetrics.engagement_rates;
    const postScores = [];

    // Process each unseen post
    for (const [postId, postData] of Object.entries(allPosts)) {
      // Skip if user has already interacted
      if (history.watched?.[postId] || history.liked?.[postId]) continue;

      // Calculate interaction score
      const interactionScore = await calculatePostInteractionScore(postId, history);
      
      // Sample from beta distributions for each engagement type
      const samples = {
        watch: sampleBeta(
          engagementRates.watch_rate * 100,
          (1 - engagementRates.watch_rate) * 100
        ),
        like: sampleBeta(
          engagementRates.like_rate * 100,
          (1 - engagementRates.like_rate) * 100
        ),
        reaction: sampleBeta(
          engagementRates.reaction_rate * 100,
          (1 - engagementRates.reaction_rate) * 100
        ),
        comment: sampleBeta(
          engagementRates.comment_rate * 100,
          (1 - engagementRates.comment_rate) * 100
        ),
        participate: sampleBeta(
          engagementRates.participate_rate * 100,
          (1 - engagementRates.participate_rate) * 100
        )
      };

      // Calculate post age and decay
      const currentTime = Date.now() / 1000;
      const postAge = currentTime - (postData.timestamp || currentTime);
      const decayFactor = Math.exp(-postAge * (decayRates[`${contentType}_decay`] || 0.5));

      // Calculate exploration-exploitation score
      const explorationScore = (
        samples.watch * postData.vectors.engagement.watch_completion_rate +
        samples.like * postData.vectors.engagement.like_rate +
        samples.reaction * postData.vectors.engagement.reaction_rate +
        samples.comment * postData.vectors.engagement.comment_rate +
        samples.participate * postData.vectors.engagement.participation_rate
      ) / 5;

      // Combine scores with decay
      const finalScore = (
        (explorationScore * 0.6) +
        (interactionScore * 0.4)
      ) * decayFactor;

      postScores.push({
        postId,
        score: finalScore,
        metrics: {
          exploration_score: explorationScore,
          interaction_score: interactionScore,
          decay_factor: decayFactor,
          predicted_engagement: samples
        },
        post_data: postData
      });
    }

    // Sort and select top recommendations
    const recommendations = postScores
      .sort((a, b) => b.score - a.score)
      .slice(0, numRecommendations)
      .map(({ postId, score, metrics, post_data }) => ({
        post_id: postId,
        score: score.toFixed(4),
        creator_id: post_data.creator_id,
        timestamp: post_data.timestamp,
        metrics: {
          predicted_engagement: Object.entries(metrics.predicted_engagement)
            .reduce((acc, [key, val]) => ({ ...acc, [key]: val.toFixed(4) }), {}),
          exploration_score: metrics.exploration_score.toFixed(4),
          interaction_score: metrics.interaction_score.toFixed(4),
          decay_factor: metrics.decay_factor.toFixed(4)
        }
      }));

    return {
      user_metrics: {
        avg_interaction_score: userMetrics.avg_interaction_score,
        engagement_rates: engagementRates,
        decay_rates: decayRates
      },
      recommendations
    };

  } catch (error) {
    console.error('Error generating recommendations:', error);
    throw error;
  }
}

/**
 * Combine and rank recommendations based on user preferences
 * @param {string} userId - User ID
 * @param {string} contentType - Content type
 * @returns {Promise<Object>} Combined and ranked recommendations
 */
async function getCombinedRecommendations(userId, contentType) {
  try {
    // Get recommendations from both sources
    const [personalizedRecs, similarPosts] = await Promise.all([
      generatePersonalizedRecommendations(userId, contentType, 15), // Get more to account for overlaps
      findSimilarPosts(userId, contentType)
    ]);

    // Get user's decay metrics for ranking
    const history = await fetchUserViewingHistory(userId, contentType);
    const decayMetrics = await calculateDecayMetrics(history, contentType);
    const userPreferenceForNew = decayMetrics[`${contentType}_decay`] > 0.5;

    // Create a map for quick lookup of personalized recommendations
    const personalizedMap = new Map(
      personalizedRecs.recommendations.map(rec => [rec.post_id, rec])
    );

    // Combine all recommendations with source tracking
    const allRecommendations = new Map();

    // Add personalized recommendations (65% weight)
    personalizedRecs.recommendations.forEach(rec => {
      allRecommendations.set(rec.post_id, {
        ...rec,
        sources: ['personalized'],
        base_score: parseFloat(rec.score),
        metrics: {
          ...rec.metrics,
          appears_in_both: false
        }
      });
    });

    // Add similar posts (35% weight) and mark overlaps
    const similarPostsList = [
      ...similarPosts.similar_posts.by_creator,
      ...similarPosts.similar_posts.by_category,
      ...similarPosts.similar_posts.by_sentiment
    ];

    similarPostsList.forEach(post => {
      if (allRecommendations.has(post.id)) {
        // Post appears in both - mark it and boost score
        const existing = allRecommendations.get(post.id);
        existing.sources.push('similar');
        existing.metrics.appears_in_both = true;
        existing.base_score *= 1.2; // 20% boost for appearing in both
      } else {
        // New post from similar recommendations
        allRecommendations.set(post.id, {
          post_id: post.id,
          creator_id: post.creator_id,
          timestamp: post.timestamp,
          sources: ['similar'],
          base_score: 0.8, // Base score for similar posts
          metrics: {
            appears_in_both: false,
            category: post.category,
            sentiment: post.sentiment,
            category_score: post.category_score,
            sentiment_score: post.sentiment_score
          }
        });
      }
    });

    // Calculate final scores and rank recommendations
    const rankedRecommendations = Array.from(allRecommendations.values())
      .map(rec => {
        const age = (Date.now() / 1000) - (rec.timestamp || Date.now() / 1000);
        const ageScore = userPreferenceForNew ? 
          Math.exp(-age * decayMetrics[`${contentType}_decay`]) :
          1 - Math.exp(-age * decayMetrics[`${contentType}_decay`]);

        // Calculate weighted score
        const sourceWeight = rec.sources.includes('personalized') ? 0.65 : 0.35;
        const overlapBonus = rec.metrics.appears_in_both ? 0.2 : 0;
        
        const finalScore = (
          rec.base_score * sourceWeight * (1 + overlapBonus) * (ageScore * 0.7 + 0.3)
        );

        return {
          ...rec,
          final_score: finalScore.toFixed(4),
          age_score: ageScore.toFixed(4)
        };
      })
      .sort((a, b) => parseFloat(b.final_score) - parseFloat(a.final_score))
      .slice(0, 10); // Get top 10 recommendations

      return {
        user_metrics: {
          ...personalizedRecs.user_metrics,
          prefers_new_content: userPreferenceForNew,
          decay_rate: decayMetrics[`${contentType}_decay`] || 0 // Provide default value
        },
        recommendations: rankedRecommendations,
        stats: {
          total_candidates: allRecommendations.size,
          overlap_count: rankedRecommendations.filter(r => r.metrics?.appears_in_both).length,
          personalized_count: rankedRecommendations.filter(r => r.sources?.includes('personalized')).length,
          similar_count: rankedRecommendations.filter(r => r.sources?.includes('similar')).length
        }
      };

    } catch (error) {
      console.error('Error combining recommendations:', error);
      // Return a safe default object
      return {
        user_metrics: {
          prefers_new_content: false,
          decay_rate: 0
        },
        recommendations: [],
        stats: {
          total_candidates: 0,
          overlap_count: 0,
          personalized_count: 0,
          similar_count: 0
        }
      };
    }
  }


  
  // Main function equivalent (if needed)
 async function main() {
    const userId = '0JzqvymiS9QsaV42RLAQIH78jjm1';
    const contentType = 'video';
    
    try {
       // Fetch history
    const history = await fetchUserViewingHistory(userId, contentType);
    
    // Calculate engagement metrics
    const metrics = await calculateUserEngagementMetrics(history, contentType);
    const analyzer = new ContentAnalyzer();
        
   
    // Fetch all posts
    const postsRef = admin.database().ref(`/${contentType}_challenges`);
    const postsSnapshot = await postsRef.once('value');
    const allPosts = postsSnapshot.val() || {};
     // Filter posts based on history
     const { postsInHistory, postsNotInHistory } = filterPostsByHistory(history, allPosts);

     console.log(`\nPosts in User's History: ${Object.keys(postsInHistory).length}`);
     console.log(`Posts not in User's History: ${Object.keys(postsNotInHistory).length}`);
 
    

      
     
      
      console.log('User Engagement Metrics')
       // Print results
       console.log('\nDecay Rates:');
       Object.entries(metrics.decay_rates).forEach(([category, rate]) => {
         console.log(`${category}: ${rate.toFixed(3)}`);
       });
       
       console.log('\nEngagement Rates:');
       Object.entries(metrics.engagement_rates).forEach(([rateType, rate]) => {
         console.log(`${rateType}: ${(rate * 100).toFixed(2)}%`);
       });
       
       console.log(`\nAverage Interaction Score: ${metrics.avg_interaction_score.toFixed(3)}`);
 
      console.log('Post Interaction Metrics')
      console.log('\nPost Interaction Scores:');
      Object.entries(metrics.post_scores).forEach(([postId, score]) => {
        console.log(`Post ${postId}: ${score.toFixed(3)}`);
      });

     
       
      console.log('\nFetching post vectors...');
      
    const postVectors = await fetchPostVectors(contentType);
    
    // Print all posts in batches of 5
    const postIds = Object.keys(postVectors);
    console.log(`\nTotal posts found: ${postIds.length}`);
    
    for (let i = 0; i < postIds.length; i += 5) {
      console.log(`\nBatch ${Math.floor(i/5) + 1}:`);
      const batchIds = postIds.slice(i, i + 5);
      batchIds.forEach(postId => {
        const data = postVectors[postId];
        console.log(`\nPost ${postId}:`);
        console.log('Creator ID:', data.metadata.creator_id);
        console.log('Description:', data.metadata.description);
        console.log('Main Post Vectors:');
        console.log('  Engagement Vector:', JSON.stringify(data.vectors.engagement, null, 2));
        console.log('  Popularity Vector:', JSON.stringify(data.vectors.popularity, null, 2));

         // Add subposts information
         if (data.subposts) {
          console.log('\nSubposts Information:');
          console.log(`  Number of Subposts: ${data.subposts.count}`);
          console.log('  Subpost Creator IDs:', data.subposts.creator_ids.join(', '));
          
          console.log('\n  Average Subpost Metrics:');
          Object.entries(data.subposts.avg_metrics).forEach(([metric, value]) => {
            console.log(`    ${metric}: ${value.toFixed(2)}`);
          });
          
          console.log('\n  Subposts Engagement Vector:');
          Object.entries(data.subposts.vectors.engagement).forEach(([metric, value]) => {
            console.log(`    ${metric}: ${value.toFixed(3)}`);
          });
          
          console.log('\n  Subposts Popularity Vector:');
          Object.entries(data.subposts.vectors.popularity).forEach(([metric, value]) => {
            console.log(`    ${metric}: ${value.toFixed(6)}`); // Using more decimal places for velocity
          });
        } else {
          console.log('\nNo subposts found');
        }
        
        console.log('\n' + '-'.repeat(50)); // Separator between posts
      });

         
    console.log('\nAnalyzing creator frequencies...');
    const creatorAnalysis = await analyzeCreatorFrequency(userId, contentType);
    
    console.log('\nCreator Frequency Analysis:');
    console.log('Statistics:', creatorAnalysis.stats);
    console.log('\nFrequent Creators (2+ appearances):');
    Object.entries(creatorAnalysis.creators)
      .forEach(([creatorId, frequency]) => {
        console.log(`Creator ${creatorId}: ${frequency} appearances`);
      });
    }
    console.time('contentAnalysis');
    const analysis = await analyzer.analyzeUserContent(history, contentType);
    console.timeEnd('contentAnalysis');
    
    console.log('\nContent Analysis Results:');
    console.log(`Analysis completed in ${analysis.analysis_time_ms}ms`);
    console.log('Dominant Categories:', analysis.dominant_categories);
    console.log('Sentiment Distribution:', analysis.sentiment_distribution);
    console.log('Content Summary:', analysis.content_summary);

    console.time('contentSummary');
    const summary = await getContentSummary(userId, contentType);
    console.timeEnd('contentSummary');
    
    console.log('\nContent Summary:', summary);
   
    console.time('similarPosts');
    const results = await findSimilarPosts(userId, contentType);
    console.timeEnd('similarPosts');
    
    console.log('\nUser Preferences:', results.user_preferences);
    
    console.log('\nSimilar Posts Found:');
    
    console.log(`\nBy Creator (${results.total_found.by_creator}):`);
    results.similar_posts.by_creator.forEach(post => {
      console.log(`- Post ID: ${post.id}`);
      console.log(`  Creator: ${post.creator_id}`);
      console.log(`  Frequency: ${post.frequency}`);
      console.log(`  Description: ${post.description?.substring(0, 100)}...`);
      console.log('  ---');
    });

    console.log(`\nBy Category (${results.total_found.by_category}):`);
    results.similar_posts.by_category.forEach(post => {
      console.log(`- Post ID: ${post.id}`);
      console.log(`  Category: ${post.category}`);
      console.log(`  Category Score: ${post.category_score}`);
      console.log(`  Description: ${post.description?.substring(0, 100)}...`);
      console.log('  ---');
    });

    console.log(`\nBy Sentiment (${results.total_found.by_sentiment}):`);
    results.similar_posts.by_sentiment.forEach(post => {
      console.log(`- Post ID: ${post.id}`);
      console.log(`  Sentiment: ${post.sentiment}`);
      console.log(`  Sentiment Score: ${post.sentiment_score}`);
      console.log(`  Description: ${post.description?.substring(0, 100)}...`);
      console.log('  ---');
    });

    console.time('recommendations');
    const { user_metrics, recommendations } = await generatePersonalizedRecommendations(userId, contentType);
    console.timeEnd('recommendations');
    
    if (!recommendations || recommendations.length === 0) {
      console.log('\nNo recommendations found for this user');
      return;
    }
    console.log('\nUser Engagement Metrics:');
    console.log('Average Interaction Score:', user_metrics.avg_interaction_score.toFixed(3));
    console.log('\nEngagement Rates:');
    Object.entries(user_metrics.engagement_rates).forEach(([type, rate]) => {
      console.log(`${type}: ${(rate * 100).toFixed(2)}%`);
    });
    
    console.log('\nDecay Rates:');
    Object.entries(user_metrics.decay_rates).forEach(([type, rate]) => {
      console.log(`${type}: ${rate.toFixed(4)}`);
    });

    console.log('\nTop Recommendations:');
    recommendations.forEach((rec, index) => {
      console.log(`\n${index + 1}. Post ID: ${rec.post_id}`);
      console.log(`   Overall Score: ${rec.score}`);
      console.log(`   Creator: ${rec.creator_id}`);
      console.log('   Predicted Engagement:');
      Object.entries(rec.metrics.predicted_engagement).forEach(([type, prob]) => {
        console.log(`     ${type}: ${prob}`);
      });
      console.log(`   Exploration Score: ${rec.metrics.exploration_score}`);
      console.log(`   Interaction Score: ${rec.metrics.interaction_score}`);
      console.log(`   Decay Factor: ${rec.metrics.decay_factor}`);
      console.log('   ---');
    });

    console.time('combinedRecommendations');
    const combinedResults = await getCombinedRecommendations(userId, contentType);
    console.timeEnd('combinedRecommendations');
    
    console.log('\nUser Metrics:');
    console.log('Prefers New Content:', combinedResults.user_metrics.prefers_new_content);
    
    // Safe access to decay rate with fallback
    const decayRate = combinedResults.user_metrics?.decay_rate ?? 0;
    console.log('Decay Rate:', decayRate.toFixed(4));
    
    if (combinedResults.recommendations && combinedResults.recommendations.length > 0) {
      console.log('\nRecommendation Stats:');
      console.log('Total Candidates:', combinedResults.stats?.total_candidates ?? 0);
      console.log('Overlapping Posts:', combinedResults.stats?.overlap_count ?? 0);
      console.log('From Personalized:', combinedResults.stats?.personalized_count ?? 0);
      console.log('From Similar:', combinedResults.stats?.similar_count ?? 0);

      console.log('\nTop Recommendations:');
      combinedResults.recommendations.forEach((rec, index) => {
        if (!rec) return; // Skip if recommendation is undefined
        
        console.log(`\n${index + 1}. Post ID: ${rec.post_id}`);
        console.log(`   Final Score: ${rec.final_score ?? '0.0000'}`);
        console.log(`   Age Score: ${rec.age_score ?? '0.0000'}`);
        console.log(`   Sources: ${rec.sources?.join(', ') ?? 'unknown'}`);
        console.log(`   Appears in Both: ${rec.metrics?.appears_in_both ?? false}`);
        console.log(`   Creator: ${rec.creator_id ?? 'unknown'}`);
        
        if (rec.metrics?.predicted_engagement) {
          console.log('   Predicted Engagement:');
          Object.entries(rec.metrics.predicted_engagement).forEach(([type, prob]) => {
            console.log(`     ${type}: ${prob ?? '0.0000'}`);
          });
        }
        console.log('   ---');
      });
    } else {
      console.log('\nNo recommendations found');
    }
    
  } catch (error) {
    console.error('Error in recommendations:', error);
    console.error('Error details:', {
      message: error.message,
      stack: error.stack
    });
  }
}
  
  // Export the functions
  module.exports = {
    calculateUserEngagementMetrics,
    fetchPostVectors, 
    analyzeCreatorFrequency,
    getContentSummary,
    cacheContentSummary,
    filterPostsByHistory,
    findSimilarPosts,
    main
  };
  
  // If you want to run the main function directly when the file is executed
  if (require.main === module) {
    main().catch(console.error);
  }

 // ... keep all existing imports and functions ...

// Replace the Cloudflare Worker export with Express routing
const express = require('express');
const cors = require('cors');
const router = express.Router();

// Configure CORS
router.use(cors());
router.use(express.json());

// Health check endpoint
router.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString()
  });
});

// Recommendations endpoint
router.all('/recommendations', async (req, res) => {
  try {
    let userId, contentType;

    if (req.method === 'GET') {
      userId = req.query.userId;
      contentType = req.query.contentType;
    } else if (req.method === 'POST') {
      userId = req.body.userId;
      contentType = req.body.contentType;
    }

    // Validate input
    if (!userId || !contentType) {
      return res.status(400).json({
        error: 'Missing required parameters',
        details: { userId, contentType }
      });
    }

    // Get recommendations
    const recommendations = await getCombinedRecommendations(userId, contentType);

    res.json({
      success: true,
      data: recommendations,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({
      error: 'Internal Server Error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Update the exports at the end of api.js
module.exports = {
  router,
  calculateUserEngagementMetrics,
  fetchPostVectors,
  analyzeCreatorFrequency,
  getContentSummary,
  cacheContentSummary,
  filterPostsByHistory,
  findSimilarPosts,
  getCombinedRecommendations  // Add this to exports
};