/**
 * Tournament Data Update System - Node.js Version
 * Fetches tournament data from external API and updates Firebase database
 * with player and club statistics, points, and tournament results
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');

// Firebase Admin Configuration
// Default service account for the default project. Keep this file local and out of version control.
const path = require('path');
const fs = require('fs');
const defaultServiceAccountPath = path.resolve(__dirname, 'serviceAccountKey.json');
if (!fs.existsSync(defaultServiceAccountPath)) {
    console.error('Missing serviceAccountKey.json next to update-node.js. Place the default project service account JSON there.');
    process.exit(1);
}
const serviceAccount = require(defaultServiceAccountPath);

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://soccerbattlehub-default-rtdb.firebaseio.com"
});

// Hold a mutable database reference so we can switch if a different DB URL is configured
let database = admin.database();
const defaultDatabase = database; // explicitly keep a handle to the default DB

/**
 * Dynamically switch the Firebase Admin DB to a configured URL if present
 * Reads config/databaseURL from the DEFAULT database, and if it differs, initializes
 * a second Admin app pointing at that database and switches the global `database` ref.
 */
async function initializeDynamicDatabase() {
    try {
        // Read from the default DB only
        const [urlSnap, saPathSnap, saJsonSnap] = await Promise.all([
            defaultDatabase.ref('config/databaseURL').once('value'),
            defaultDatabase.ref('config/serviceAccountKeyPath').once('value'),
            defaultDatabase.ref('config/serviceAccountKeyJson').once('value')
        ]);
        const configuredUrl = urlSnap.val();
        const alternateSaPath = saPathSnap.val();
        const inlineSaJson = saJsonSnap.val();
        const defaultUrl = 'https://soccerbattlehub-default-rtdb.firebaseio.com';

        if (configuredUrl && typeof configuredUrl === 'string' && configuredUrl !== defaultUrl) {
            // Initialize or reuse a named app for the configured DB
            let targetApp;
            try {
                targetApp = admin.app('dynamicDatabase');
            } catch {
                // If switching to a different project, allow specifying a different service account via JSON or file path
                let credentialToUse = admin.credential.cert(serviceAccount);
                let usedAlternateCredential = false;

                // Priority 1: Inline JSON from config
                if (inlineSaJson) {
                    try {
                        const obj = (typeof inlineSaJson === 'string') ? JSON.parse(inlineSaJson) : inlineSaJson;
                        if (obj && obj.client_email && obj.private_key) {
                            credentialToUse = admin.credential.cert(obj);
                            console.log(`Using inline service account JSON from config for ${configuredUrl}`);
                            usedAlternateCredential = true;
                        } else {
                            console.warn('Inline service account JSON missing required fields (client_email/private_key).');
                        }
                    } catch (e) {
                        console.warn(`Failed to parse inline service account JSON: ${e.message}.`);
                    }
                }

                // Priority 2: File path
                if (!usedAlternateCredential && alternateSaPath && typeof alternateSaPath === 'string') {
                    try {
                        const resolved = path.isAbsolute(alternateSaPath)
                            ? alternateSaPath
                            : path.resolve(process.cwd(), alternateSaPath);
                        if (fs.existsSync(resolved)) {
                            const altSa = require(resolved);
                            credentialToUse = admin.credential.cert(altSa);
                            console.log(`Using alternate service account at ${resolved} for ${configuredUrl}`);
                            usedAlternateCredential = true;
                        } else {
                            console.warn(`Alternate service account path not found: ${resolved}.`);
                        }
                    } catch (e) {
                        console.warn(`Failed to load alternate service account from path: ${e.message}.`);
                    }
                }

                targetApp = admin.initializeApp({
                    credential: credentialToUse,
                    databaseURL: configuredUrl
                }, 'dynamicDatabase');
            }

            database = targetApp.database();
            console.log(`Using configured database: ${configuredUrl}`);
        } else {
            database = defaultDatabase;
            console.log(`Using default database: ${defaultUrl}`);
        }
    } catch (err) {
        // On any error, fall back to default DB
        database = defaultDatabase;
        console.warn(`Dynamic database check failed, using default DB. Reason: ${err.message}`);
    }
}

// API Configuration - Using environment variables for security
const API_CONFIG = {
    accessToken: process.env.API_ACCESS_TOKEN || '',
    appId: process.env.API_APP_ID || '',
    baseUrl: process.env.API_BASE_URL || 'https://backbone-client-api.azurewebsites.net/api/v1',
    tournamentsDays: parseInt(process.env.TOURNAMENTS_DAYS) || 1 // How many days back to fetch tournaments
};

// PlayFab Configuration - For fetching player profiles and statistics
const PLAYFAB_CONFIG = {
    titleId: process.env.PLAYFAB_TITLE_ID || '',
    secretKey: process.env.PLAYFAB_SECRET_KEY || '',
    baseUrl: 'https://{titleId}.playfabapi.com'
};

// Validate required environment variables
if (!API_CONFIG.accessToken || !API_CONFIG.appId) {
    console.error('❌ Missing required environment variables: API_ACCESS_TOKEN and/or API_APP_ID');
    console.log('Please set the following environment variables:');
    console.log('- API_ACCESS_TOKEN: Your access token');
    console.log('- API_APP_ID: Your application ID');
    console.log('- API_BASE_URL: API base URL (optional, defaults to current URL)');
    console.log('- TOURNAMENTS_DAYS: Number of days back to fetch (optional, defaults to 1)');
    process.exit(1);
}

// Validate PlayFab environment variables (optional - if not set, player sync will be skipped)
const PLAYFAB_ENABLED = !!(PLAYFAB_CONFIG.titleId && PLAYFAB_CONFIG.secretKey);
if (!PLAYFAB_ENABLED) {
    console.warn('⚠️ PlayFab credentials not set. Player name/trophy sync will be skipped.');
    console.log('To enable PlayFab sync, set these environment variables:');
    console.log('- PLAYFAB_TITLE_ID: Your PlayFab Title ID');
    console.log('- PLAYFAB_SECRET_KEY: Your PlayFab Developer Secret Key');
}

// Global Variables
let playersCache = {}; // Cache for player data to avoid repeated database calls
let currentSeason = 1; // Current season number

/**
 * Load current season from Firebase
 */
async function loadCurrentSeason() {
    try {
        const snapshot = await database.ref('settings/currentSeason').once('value');
        currentSeason = snapshot.val() || 1;
        console.log(`Current season loaded: ${currentSeason}`);
    } catch (error) {
        console.error('Error loading current season:', error);
        currentSeason = 1; // Fallback to season 1
    }
}

/**
 * Gets the PlayFab API URL for a given endpoint
 * @param {string} endpoint - API endpoint path
 * @returns {string} Full PlayFab API URL
 */
function getPlayFabUrl(endpoint) {
    return PLAYFAB_CONFIG.baseUrl.replace('{titleId}', PLAYFAB_CONFIG.titleId) + endpoint;
}

/**
 * Fetches a player's profile from PlayFab Server API
 * @param {string} playfabId - The player's PlayFab ID
 * @returns {Object|null} Player profile with DisplayName, or null if failed
 */
async function getPlayFabPlayerProfile(playfabId) {
    if (!PLAYFAB_ENABLED) return null;

    try {
        const response = await fetch(getPlayFabUrl('/Server/GetPlayerProfile'), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-SecretKey': PLAYFAB_CONFIG.secretKey
            },
            body: JSON.stringify({
                PlayFabId: playfabId,
                ProfileConstraints: {
                    ShowDisplayName: true,
                    ShowStatistics: true
                }
            })
        });

        if (!response.ok) {
            return null;
        }

        const result = await response.json();
        if (result.code === 200 && result.data?.PlayerProfile) {
            return result.data.PlayerProfile;
        }
        return null;
    } catch (error) {
        console.error(`Error fetching PlayFab profile for ${playfabId}:`, error.message);
        return null;
    }
}

/**
 * Fetches a player's statistics from PlayFab Server API
 * @param {string} playfabId - The player's PlayFab ID
 * @param {string[]} statisticNames - Names of statistics to fetch (e.g., ['Trophies', 'TrophyCount'])
 * @returns {Object|null} Statistics object, or null if failed
 */
async function getPlayFabPlayerStatistics(playfabId, statisticNames = ['Trophies']) {
    if (!PLAYFAB_ENABLED) return null;

    try {
        const response = await fetch(getPlayFabUrl('/Server/GetPlayerStatistics'), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-SecretKey': PLAYFAB_CONFIG.secretKey
            },
            body: JSON.stringify({
                PlayFabId: playfabId,
                StatisticNames: statisticNames
            })
        });

        if (!response.ok) {
            return null;
        }

        const result = await response.json();
        if (result.code === 200 && result.data?.Statistics) {
            // Convert array to object for easier access
            const stats = {};
            result.data.Statistics.forEach(stat => {
                stats[stat.StatisticName] = stat.Value;
            });
            return stats;
        }
        return null;
    } catch (error) {
        console.error(`Error fetching PlayFab statistics for ${playfabId}:`, error.message);
        return null;
    }
}

/**
 * Fetches player data from PlayFab and updates Firebase for all club players
 * Updates player names and trophy counts from PlayFab
 */
async function syncPlayFabPlayerData() {
    if (!PLAYFAB_ENABLED) {
        console.log('⏭️ PlayFab sync skipped (credentials not configured)');
        return;
    }

    console.log('🔄 Starting PlayFab player data sync...');
    
    try {
        const clubSnapshot = await database.ref('clubs').once('value');
        const clubs = clubSnapshot.val() || {};
        
        let totalPlayers = 0;
        let updatedPlayers = 0;
        let failedPlayers = 0;
        const updates = {};

        // Collect all players from all clubs
        for (const [clubId, clubData] of Object.entries(clubs)) {
            if (!clubData.players) continue;

            for (const [playerKey, playerData] of Object.entries(clubData.players)) {
                if (!playerData.playfabId) continue;
                
                totalPlayers++;
                const normalizedId = playerData.playfabId.toUpperCase().replace(/\s/g, '');
                
                try {
                    // Fetch profile and statistics in parallel
                    const [profile, statistics] = await Promise.all([
                        getPlayFabPlayerProfile(normalizedId),
                        getPlayFabPlayerStatistics(normalizedId, ['NUM_TROPHIES_SEASON'])
                    ]);

                    let hasUpdates = false;
                    const playerUpdates = {};

                    // Update display name if available and different
                    if (profile?.DisplayName && profile.DisplayName !== playerData.name) {
                        playerUpdates.name = profile.DisplayName;
                        hasUpdates = true;
                    }

                    // Update trophy count if available (statistic is named NUM_TROPHIES_SEASON in PlayFab)
                    if (statistics?.NUM_TROPHIES_SEASON !== undefined) {
                        const newTrophyCount = statistics.NUM_TROPHIES_SEASON;
                        if (newTrophyCount !== playerData.trophyCount) {
                            playerUpdates.trophyCount = newTrophyCount;
                            hasUpdates = true;
                        }
                    }

                    if (hasUpdates) {
                        // Queue updates for club player data (just trophyCount, no season-specific here)
                        updates[`clubs/${clubId}/players/${playerKey}`] = {
                            ...playerData,
                            ...playerUpdates,
                            lastPlayFabSync: new Date().toISOString()
                        };
                        
                        // Also update the players table with season-specific trophyCount (like app.js does)
                        if (playerUpdates.trophyCount !== undefined) {
                            updates[`players/${normalizedId}/trophyCount`] = playerUpdates.trophyCount;
                            updates[`players/${normalizedId}/seasons/${currentSeason}/trophyCount`] = playerUpdates.trophyCount;
                        }
                        if (playerUpdates.name) {
                            updates[`players/${normalizedId}/name`] = playerUpdates.name;
                        }
                        
                        updatedPlayers++;
                        console.log(`  ✓ ${normalizedId}: ${playerUpdates.name ? `name="${playerUpdates.name}"` : ''} ${playerUpdates.trophyCount !== undefined ? `trophies=${playerUpdates.trophyCount}` : ''}`);
                    }

                    // Small delay to avoid rate limiting
                    await new Promise(resolve => setTimeout(resolve, 100));

                } catch (error) {
                    console.error(`  ✗ Failed to sync ${normalizedId}: ${error.message}`);
                    failedPlayers++;
                }
            }
        }

        // Apply all updates in a single batch
        if (Object.keys(updates).length > 0) {
            // Process updates in batches to avoid Firebase limits
            const updateEntries = Object.entries(updates);
            const batchSize = 100;
            
            for (let i = 0; i < updateEntries.length; i += batchSize) {
                const batch = updateEntries.slice(i, i + batchSize);
                const batchUpdates = Object.fromEntries(batch);
                await database.ref().update(batchUpdates);
            }
        }

        console.log(`✅ PlayFab sync complete: ${updatedPlayers}/${totalPlayers} players updated, ${failedPlayers} failed`);

        // Now update club total trophies by summing all player trophy counts
        console.log('🏆 Updating club total trophies...');
        
        // Re-fetch clubs to get updated player data
        const updatedClubSnapshot = await database.ref('clubs').once('value');
        const updatedClubs = updatedClubSnapshot.val() || {};
        
        const clubTrophyUpdates = {};
        let clubsUpdated = 0;
        
        for (const [clubId, clubData] of Object.entries(updatedClubs)) {
            if (!clubData.players) continue;
            
            // Sum up all player trophy counts
            let clubTotalTrophies = 0;
            for (const playerData of Object.values(clubData.players)) {
                clubTotalTrophies += playerData.trophyCount || 0;
            }
            
            // Only update if different from current value
            const currentClubTrophies = clubData.totalTrophies || 0;
            const currentSeasonTrophies = clubData.seasons?.[currentSeason]?.totalTrophies || 0;
            
            if (clubTotalTrophies !== currentClubTrophies || clubTotalTrophies !== currentSeasonTrophies) {
                // Update overall totalTrophies (current season trophies)
                clubTrophyUpdates[`clubs/${clubId}/totalTrophies`] = clubTotalTrophies;
                clubTrophyUpdates[`clubs_summary/${clubId}/totalTrophies`] = clubTotalTrophies;
                // Also update season-specific totalTrophies
                clubTrophyUpdates[`clubs/${clubId}/seasons/${currentSeason}/totalTrophies`] = clubTotalTrophies;
                console.log(`  ✓ ${clubData.name}: ${currentClubTrophies} → ${clubTotalTrophies} trophies (season ${currentSeason})`);
                clubsUpdated++;
            }
        }
        
        // Apply club trophy updates
        if (Object.keys(clubTrophyUpdates).length > 0) {
            await database.ref().update(clubTrophyUpdates);
        }
        
        console.log(`✅ Club trophies updated: ${clubsUpdated} clubs`);
        
    } catch (error) {
        console.error('Error during PlayFab sync:', error);
    }
}

/**
 * Fetches all clubs and their players from Firebase
 * Creates a normalized player lookup cache for quick access
 * @returns {Object} Player lookup object with normalized PlayFab IDs as keys
 */
async function fetchClubsAndPlayers() {
    try {
        const clubRef = database.ref('clubs');
        const snapshot = await clubRef.once('value');
        const clubs = snapshot.val() || {};
        
        // Reset the global cache
        playersCache = {};

        // Build player lookup cache
        Object.entries(clubs).forEach(([clubId, clubData]) => {
            if (!clubData.name || !clubData.players) return;
            
            const clubName = clubData.name;
            const playersInClub = clubData.players;

            Object.entries(playersInClub).forEach(([playerId, playerData]) => {
                if (!playerData.playfabId || !playerData.name) return;
                
                // Normalize PlayFab ID: uppercase and remove spaces
                const normalizedPlayfabId = playerData.playfabId.toUpperCase().replace(/\s/g, '');

                playersCache[normalizedPlayfabId] = {
                    name: playerData.name,
                    clubName: clubName,
                    clubId: clubId,
                    playerId: playerId
                };
            });
        });

        console.log(`Loaded ${Object.keys(playersCache).length} players from ${Object.keys(clubs).length} clubs`);
        return playersCache;
    } catch (error) {
        console.error('Error fetching clubs and players:', error);
        throw error;
    }
}

/**
 * Main initialization function that starts the tournament fetching process
 */
async function initializeTournamentFetcher() {
    try {
        // Pick the correct database before any reads/writes
        await initializeDynamicDatabase();
        
        // Load current season first
        await loadCurrentSeason();
        
        // Process tournaments first
        console.log(`Starting tournament fetcher for Season ${currentSeason}...`);
        await fetchAndProcessTournaments();
        
        // Then sync player data from PlayFab (names and trophies)
        await syncPlayFabPlayerData();
    } catch (error) {
        console.error('Failed to initialize tournament fetcher:', error);
        console.log('Error: Failed to initialize tournament system');
    }
}

/**
 * Fetches tournaments from the last few days and processes new ones
 */
async function fetchAndProcessTournaments() {
    try {
        // Calculate date range
        const currentDate = new Date();
        const pastDate = new Date(currentDate);
        pastDate.setDate(currentDate.getDate() - API_CONFIG.tournamentsDays);

        const untilDate = currentDate.toISOString();
        const sinceDate = pastDate.toISOString();

        console.log(`Fetching tournaments from ${sinceDate.split('T')[0]} to ${untilDate.split('T')[0]}...`);

        // Prepare API request
        const url = `${API_CONFIG.baseUrl}/dataReportGetTournaments`;
        const headers = {
            'BACKBONE_APP_ID': API_CONFIG.appId,
            'Accept-Encoding': 'gzip',
            'Content-Type': 'application/x-www-form-urlencoded'
        };

        const body = new URLSearchParams({
            'accessToken': API_CONFIG.accessToken,
            'sinceDate': sinceDate,
            'untilDate': untilDate
        });

        // Fetch tournaments
        const response = await fetch(url, {
            method: 'POST',
            headers: headers,
            body: body
        });

        if (!response.ok) {
            throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
        }

        const tournaments = await response.json();
        
        if (!Array.isArray(tournaments)) {
            throw new Error('Invalid tournament data received');
        }

        console.log(`Found ${tournaments.length} tournaments to check`);

        // Process each tournament
        let processedCount = 0;
        let skippedCount = 0;

        for (const tournament of tournaments) {
            const tier = getTierFromName(tournament.name);
            const alreadyFetched = await isTournamentAlreadyProcessed(tournament.tournamentId);
            
            if (!alreadyFetched) {
                console.log(`Processing: ${tournament.name} (Tier: ${tier})`);
                await fetchAndProcessTournamentData(tournament.tournamentId, tier, tournament.name);
                await markTournamentAsProcessed(tournament.tournamentId);
                processedCount++;
            } else {
                skippedCount++;
            }
        }

        console.log(`✅ Finished! Processed: ${processedCount}, Skipped: ${skippedCount}`);
        console.log(`Individual results tracked for all top 4 players regardless of club membership`);
        
    } catch (error) {
        console.error('Error fetching tournaments:', error);
        console.log(`❌ Error: ${error.message}`);
    }
}

/**
 * Determines tournament tier from tournament name using pattern matching
 * @param {string} name - Tournament name to analyze
 * @returns {string} Tournament tier in camelCase format
 */
function getTierFromName(name) {
    if (!name || typeof name !== 'string') {
        return 'Unknown';
    }

    const normalizedName = name.toLowerCase();
    
    // Silver tier tournaments (most specific first)
    if (normalizedName.includes('silver') && normalizedName.includes('singapore')) {
        return 'silverAsia1';
    }
    if (normalizedName.includes('silver') && normalizedName.includes('eu') && normalizedName.includes('1')) {
        return 'silverEu1';
    }
    if (normalizedName.includes('silver') && normalizedName.includes('eu') && normalizedName.includes('2')) {
        return 'silverEu2';
    }
    if (normalizedName.includes('silver') && normalizedName.includes('us') && normalizedName.includes('1')) {
        return 'silverUs1';
    }
    if (normalizedName.includes('silver') && normalizedName.includes('us') && normalizedName.includes('2')) {
        return 'silverUs2';
    }
    
    // Special tournaments
    if (normalizedName.includes('gold') && normalizedName.includes('limited')) {
        return 'goldLimited';
    }
    if (normalizedName.includes('halloween')) {
        return 'halloweenCup';
    }
    if (normalizedName.includes('asia cup')) {
        return 'asia';
    }
    if (normalizedName.includes('round madness') && normalizedName.includes('eu')) {
        return 'roundMadnessEu';
    }
    if (normalizedName.includes('round madness') && normalizedName.includes('us')) {
        return 'roundMadnessUs';
    }
    if (normalizedName.includes('round madness') && normalizedName.includes('india')) {
        return 'roundMadnessIndia';
    }
    
    // Premium tournaments (order matters for specificity)
    if (normalizedName.includes('diamond')) {
        return 'diamond';
    }
    if (normalizedName.includes('platinum')) {
        return 'platinum';
    }
    if (normalizedName.includes('gold')) {
        return 'gold';
    }
    if (normalizedName.includes('america')) {
        return 'america';
    }
    if (normalizedName.includes('championship')) {
        return 'Unknown'; // Championship tournaments are not currently tracked
    }
    
    // If no pattern matched, return Unknown
    return 'Unknown';
}

/**
 * Checks if a tournament has already been processed
 * @param {string} tournamentId - Tournament ID to check
 * @returns {boolean} True if tournament was already processed
 */
async function isTournamentAlreadyProcessed(tournamentId) {
    try {
        const snapshot = await database.ref(`fetchedTournaments/${tournamentId}`).once('value');
        return snapshot.exists();
    } catch (error) {
        console.error('Error checking tournament status:', error);
        return false;
    }
}

/**
 * Marks a tournament as processed to avoid duplicate processing
 * @param {string} tournamentId - Tournament ID to mark as processed
 */
async function markTournamentAsProcessed(tournamentId) {
    try {
        await database.ref(`fetchedTournaments/${tournamentId}`).set({
            fetchedAt: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error marking tournament as processed:', error);
        throw error;
    }
}

/**
 * Fetches tournament participant data from API and processes it
 * @param {string} tournamentId - Tournament ID to fetch
 * @param {string} tier - Tournament tier/category
 * @param {string} tournamentName - Tournament name for logging
 */
async function fetchAndProcessTournamentData(tournamentId, tier, tournamentName) {
    try {
        const response = await fetch(`${API_CONFIG.baseUrl}/dataReportGetTournamentUsers`, {
            method: 'POST',
            headers: {
                'BACKBONE_APP_ID': API_CONFIG.appId,
                'Accept-Encoding': 'gzip',
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: new URLSearchParams({
                accessToken: API_CONFIG.accessToken,
                tournamentId: tournamentId
            })
        });

        if (!response.ok) {
            throw new Error(`Failed to fetch tournament data: ${response.status} ${response.statusText}`);
        }

        const tournamentData = await response.json();

        if (!Array.isArray(tournamentData)) {
            throw new Error('Invalid tournament data format received');
        }

        await processTournamentResults(tournamentData, tier, tournamentName);

    } catch (error) {
        console.error(`Error processing tournament ${tournamentId}:`, error);
        console.log(`❌ Failed to process ${tournamentName}: ${error.message}`);
        throw error;
    }
}

/**
 * Processes tournament results and updates player/club statistics
 * @param {Array} tournamentData - Raw tournament data from API
 * @param {string} tier - Tournament tier
 * @param {string} tournamentName - Tournament name for logging
 */
async function processTournamentResults(tournamentData, tier, tournamentName) {
    try {
        // Load fresh player data
        const players = await fetchClubsAndPlayers();
        
        // Group players by team (partyId)
        const teams = groupPlayersByTeam(tournamentData, players);
        
        // Sort teams by placement for fair processing
        const sortedTeams = sortTeamsByPlacement(teams);
        
        // Get points mapping for this tournament tier
        const pointsForPlacement = getPointsMapping(tier);
        
        console.log(`Processing ${Object.keys(sortedTeams).length} teams for ${tournamentName}`);
        
        // Display top 4 teams with player IDs
        displayTop4Teams(sortedTeams, tournamentName, players);
        
        // Track which clubs have already been awarded to prevent duplicates
        const clubsAlreadyAwarded = new Set();
        let teamsProcessed = 0;
        let individualResultsRecorded = 0;
        
        // Process each team
        for (const [teamId, team] of Object.entries(sortedTeams)) {
            const processResult = await processTeamResults(
                team, 
                pointsForPlacement, 
                tier, 
                clubsAlreadyAwarded,
                players
            );
            
            if (processResult.processed) {
                teamsProcessed++;
                console.log(`✓ ${processResult.clubName}: ${processResult.pointsAwarded} points (${processResult.placement}${getPlacementSuffix(processResult.placement)} place)`);
            }
            
            // Track individual results recorded
            individualResultsRecorded += processResult.individualResults || 0;
        }
        
        console.log(`Successfully processed ${teamsProcessed} teams and ${individualResultsRecorded} individual results for ${tournamentName}`);
        
    } catch (error) {
        console.error(`Error processing tournament results for ${tournamentName}:`, error);
        throw error;
    }
}

/**
 * Groups tournament participants by team (partyId)
 * @param {Array} tournamentData - Raw tournament data
 * @param {Object} players - Player lookup cache
 * @returns {Object} Teams grouped by partyId
 */
function groupPlayersByTeam(tournamentData, players) {
    const teams = {};

    tournamentData.forEach(row => {
        const playerId = row.userPlayfabId?.toUpperCase();
        const placement = parseInt(row.userPlace);
        const partyId = row.partyId;

        // Skip invalid data
        if (!playerId || !partyId || isNaN(placement) || placement < 1 || placement > 4) {
            return;
        }

        // Get player info if available, otherwise use default values
        const playerInfo = players[playerId] || {
            name: 'Unknown Player',
            clubName: 'No Club',
            clubId: null,
            playerId: null
        };

        // Initialize team if not exists
        if (!teams[partyId]) {
            teams[partyId] = {
                placements: [],
                clubNames: [],
                playerIds: [],
                playerNames: [],
                hasUnregisteredPlayers: false
            };
        }

        // Track if this team has unregistered players
        if (!players[playerId]) {
            teams[partyId].hasUnregisteredPlayers = true;
        }

        // Add player to team
        teams[partyId].clubNames.push(playerInfo.clubName);
        teams[partyId].placements.push(placement);
        teams[partyId].playerIds.push(playerId);
        teams[partyId].playerNames.push(playerInfo.name);
    });

    return teams;
}

/**
 * Sorts teams by their best placement
 * @param {Object} teams - Teams to sort
 * @returns {Object} Sorted teams
 */
function sortTeamsByPlacement(teams) {
    return Object.entries(teams)
        .sort(([, teamA], [, teamB]) => {
            const bestPlacementA = Math.min(...teamA.placements);
            const bestPlacementB = Math.min(...teamB.placements);
            return bestPlacementA - bestPlacementB;
        })
        .reduce((acc, [partyId, team]) => {
            acc[partyId] = team;
            return acc;
        }, {});
}

/**
 * Displays the top 4 teams with their player IDs and information
 * @param {Object} sortedTeams - Teams sorted by placement
 * @param {string} tournamentName - Tournament name for logging
 * @param {Object} players - Player lookup cache
 */
function displayTop4Teams(sortedTeams, tournamentName, players) {
    const teamEntries = Object.entries(sortedTeams);
    const top4Teams = teamEntries.slice(0, 4);
    
    console.log(`🏆 Top 4 Teams in ${tournamentName}:`);
    
    top4Teams.forEach(([teamId, team], index) => {
        const placement = Math.min(...team.placements);
        const placementSuffix = getPlacementSuffix(placement);
        const uniqueClubs = [...new Set(team.clubNames.filter(club => club !== 'No Club'))];
        const hasUnregisteredPlayers = team.hasUnregisteredPlayers || false;
        
        let teamComposition;
        if (hasUnregisteredPlayers && uniqueClubs.length === 0) {
            teamComposition = 'Unregistered Players';
        } else if (hasUnregisteredPlayers && uniqueClubs.length > 0) {
            teamComposition = uniqueClubs.length === 1 ? `${uniqueClubs[0]} + Unregistered` : 'Mixed Team + Unregistered';
        } else {
            teamComposition = uniqueClubs.length === 1 ? uniqueClubs[0] : uniqueClubs.length > 1 ? 'Mixed Team' : 'No Club';
        }
        
        console.log(`  ${placement}${placementSuffix} Place (Team ${teamId}) - ${teamComposition}:`);
        
        team.playerIds.forEach((playerId, playerIndex) => {
            const playerInfo = players[playerId];
            const playerName = playerInfo ? playerInfo.name : 'Unregistered Player';
            const clubName = playerInfo ? playerInfo.clubName : 'No Club';
            const individualPlacement = team.placements[playerIndex];
            const clubRegisteredStatus = playerInfo ? '✓' : '✗';
            const individualResultStatus = '📊'; // All players get individual results tracked
            const registrationStatus = playerInfo ? 'Registered' : 'Unregistered';
            
            console.log(`    Player ${playerIndex + 1}: ${playerId} (${playerName}) - ${clubName} - ${individualPlacement}${getPlacementSuffix(individualPlacement)} place [Club: ${clubRegisteredStatus}] [Individual: ${individualResultStatus}] [${registrationStatus}]`);
        });
        
        console.log(''); // Empty line for readability
    });
}

/**
 * Gets point values for tournament placements
 * @param {string} tier - Tournament tier
 * @returns {Array} Points array [1st, 2nd, 3rd, 4th]
 */
function getPointsMapping(tier) {
    const pointsMap = {
        diamond: [36, 18, 9, 9],
        platinum: [18, 9, 4.5, 4.5],
        goldLimited: [6, 3, 1.5, 1.5],
        gold: [5, 2.5, 1.2, 1.2],
        silverEu2: [2, 1, 0.5, 0.5],
        silverEu1: [1.6, 0.8, 0.4, 0.4],
        silverUs1: [1.4, 0.7, 0.3, 0.3],
        asia: [1.2, 0.6, 0.3, 0.3],
        america: [2, 1, 0.5, 0.5],
        silverUs2: [1.2, 0.6, 0, 0],
        silverAsia1: [1.8, 0.9, 0.4, 0.4],
        roundMadnessEu: [10, 5, 2.5, 1.7],
        roundMadnessUs: [7, 3.5, 1.7, 1.2],
        roundMadnessIndia: [6.4, 3.2, 1.6, 1.1],
        halloweenCup: [10, 5, 2.5, 2.5],
        Unknown: [0, 0, 0, 0]
    };
    
    return pointsMap[tier] || [0, 0, 0, 0];
}

/**
 * Processes a single team's results and updates database
 * @param {Object} team - Team data
 * @param {Array} pointsForPlacement - Points array for this tier
 * @param {string} tier - Tournament tier
 * @param {Set} clubsAlreadyAwarded - Set of clubs already awarded
 * @param {Object} players - Player lookup cache
 * @returns {Object} Processing result
 */
async function processTeamResults(team, pointsForPlacement, tier, clubsAlreadyAwarded, players) {
    const uniqueClubs = new Set(team.clubNames.filter(club => club !== 'No Club'));
    const isMixedTeam = uniqueClubs.size > 1;
    const hasThreePlayers = team.playerIds.length === 3;
    const placement = team.placements[0];
    const pointsAwarded = pointsForPlacement[placement - 1] || 0;

    // Check if all 3 players are from the same registered club
    const allPlayersFromSameClub = hasThreePlayers && 
                                   uniqueClubs.size === 1 && 
                                   team.clubNames.filter(club => club !== 'No Club').length === 3;

    try {
        // ALWAYS track individual results for all players in top 4 teams (in separate players collection)
        for (let i = 0; i < team.playerIds.length; i++) {
            const playerId = team.playerIds[i];
            const individualPlacement = team.placements[i];
            const individualPoints = pointsForPlacement[individualPlacement - 1] || 0;
            
            // Record individual result regardless of club status (in players collection)
            await updateIndividualPlayerResult(playerId, tier, individualPlacement, individualPoints);
        }

        // Update player stats in clubs ONLY if all 3 players are from the same registered club
        if (allPlayersFromSameClub) {
            for (let i = 0; i < team.playerIds.length; i++) {
                const playerId = team.playerIds[i];
                const individualPlacement = team.placements[i];
                const individualPoints = pointsForPlacement[individualPlacement - 1] || 0;
                
                // Update player points and stats in their club
                if (players[playerId]) {
                    await updatePlayerPoints(playerId, individualPoints);
                    await updatePlayerTournamentStats(playerId, tier, individualPlacement);
                }
            }
        }

        // Process club results only for teams where ALL 3 players are from the same registered club
        if (allPlayersFromSameClub) {
            const clubName = team.clubNames.find(club => club !== 'No Club');
            
            // Skip if club already awarded in this tournament
            if (clubName && !clubsAlreadyAwarded.has(clubName)) {
                // Update club
                await updateClubPoints(clubName, pointsAwarded);
                await updateClubTournamentStats(clubName, tier, placement);

                // Mark club as awarded
                clubsAlreadyAwarded.add(clubName);

                return {
                    processed: true,
                    clubName,
                    pointsAwarded,
                    placement,
                    playerCount: team.playerIds.length,
                    individualResults: team.playerIds.length
                };
            }
        }

        return {
            processed: false,
            reason: !hasThreePlayers ? 'Invalid team size' : 
                   !allPlayersFromSameClub ? 'Not all players from same registered club' : 
                   'Club already awarded',
            individualResults: team.playerIds.length
        };

    } catch (error) {
        console.error(`Error processing team results:`, error);
        return { processed: false, reason: error.message, individualResults: 0 };
    }
}

/**
 * Helper function to get placement suffix (1st, 2nd, 3rd, 4th)
 * @param {number} placement - Placement number
 * @returns {string} Placement with suffix
 */
function getPlacementSuffix(placement) {
    const suffixes = { 1: 'st', 2: 'nd', 3: 'rd' };
    return suffixes[placement] || 'th';
}

/**
 * Updates club's total points
 * @param {string} clubName - Name of the club
 * @param {number} points - Points to add
 */
async function updateClubPoints(clubName, points) {
    try {
        const clubSnapshot = await database.ref('clubs').once('value');
        const clubs = clubSnapshot.val() || {};

        for (const [clubId, clubData] of Object.entries(clubs)) {
            if (clubData.name === clubName) {
                const clubRef = database.ref(`clubs/${clubId}`);
                const currentData = await clubRef.once('value');
                
                // Update overall total points (for backward compatibility)
                const currentTotalPoints = currentData.val()?.totalPoints || 0;
                
                // Update season-specific points
                const currentSeasonPoints = currentData.val()?.seasons?.[currentSeason]?.totalPoints || 0;
                
                const updates = {};
                updates.totalPoints = currentTotalPoints + points;
                updates[`seasons/${currentSeason}/totalPoints`] = currentSeasonPoints + points;
                
                await clubRef.update(updates);
                
                // Update clubs_summary with new total points
                await database.ref(`clubs_summary/${clubId}/totalPoints`).set(currentTotalPoints + points);
                
                console.log(`Updated ${clubName} points: +${points} (season ${currentSeason}: ${currentSeasonPoints + points}, total: ${currentTotalPoints + points})`);
                return;
            }
        }
        
        console.warn(`Club "${clubName}" not found for points update`);
    } catch (error) {
        console.error(`Error updating club points for ${clubName}:`, error);
        throw error;
    }
}

/**
 * Updates club's tournament statistics
 * @param {string} clubName - Name of the club
 * @param {string} tier - Tournament tier
 * @param {number} placement - Team placement (1st, 2nd, 3rd, 4th)
 */
async function updateClubTournamentStats(clubName, tier, placement) {
    if (tier === 'Unknown') return;

    try {
        const clubSnapshot = await database.ref('clubs').once('value');
        const clubs = clubSnapshot.val() || {};

        for (const [clubId, clubData] of Object.entries(clubs)) {
            if (clubData.name === clubName) {
                // Update season-specific stats
                const seasonTierRef = database.ref(`clubs/${clubId}/seasons/${currentSeason}/${tier}`);
                // Update overall/total stats
                const overallTierRef = database.ref(`clubs/${clubId}/${tier}`);
                
                // Initialize tier stats if not exists for both season and overall
                await initializeTournamentStats(seasonTierRef);
                await initializeTournamentStats(overallTierRef);
                
                // Update appropriate placement counter
                const statField = getStatFieldForPlacement(placement);
                if (statField) {
                    // Update season-specific stats
                    const currentSeasonData = await seasonTierRef.once('value');
                    const currentSeasonValue = currentSeasonData.val()?.[statField] || 0;
                    
                    // Update overall/total stats
                    const currentOverallData = await overallTierRef.once('value');
                    const currentOverallValue = currentOverallData.val()?.[statField] || 0;
                    
                    // Perform both updates
                    await Promise.all([
                        seasonTierRef.update({
                            [statField]: currentSeasonValue + 1
                        }),
                        overallTierRef.update({
                            [statField]: currentOverallValue + 1
                        })
                    ]);
                    
                    console.log(`Updated ${clubName} ${tier} stats: ${statField} +1 (season ${currentSeason}: ${currentSeasonValue + 1}, total: ${currentOverallValue + 1})`);
                }

                return;
            }
        }

        console.warn(`Club "${clubName}" not found for tournament stats update`);
    } catch (error) {
        console.error(`Error updating club tournament stats for ${clubName}:`, error);
        throw error;
    }
}

/**
 * Updates individual player results regardless of club membership
 * @param {string} playerId - Normalized PlayFab ID
 * @param {string} tier - Tournament tier
 * @param {number} placement - Player placement
 * @param {number} points - Points awarded
 */
async function updateIndividualPlayerResult(playerId, tier, placement, points) {
    if (tier === 'Unknown') return;

    try {
        const playerResultRef = database.ref(`players/${playerId}`);
        
        // Check if player is registered in players
        const existingPlayerSnapshot = await database.ref(`players/${playerId}`).once('value');
        const existingPlayerData = existingPlayerSnapshot.val();
        const isRegistered = existingPlayerData ? existingPlayerData.isRegistered || false : false;
        
        // Initialize player's individual results if they don't exist
        const playerSnapshot = await playerResultRef.once('value');
        if (!playerSnapshot.exists()) {
            const initialData = {
                totalPoints: 0,
                seasons: {},
                isRegistered: isRegistered
            };
            
            // Only add name and trophyCount if player is registered
            if (isRegistered && existingPlayerData) {
                initialData.name = existingPlayerData.name;
                initialData.trophyCount = existingPlayerData.trophyCount;
            }
            
            await playerResultRef.set(initialData);
        } else {
            // Update registration status if it has changed
            const existingData = playerSnapshot.val();
            if (existingData.isRegistered !== isRegistered) {
                const updates = { isRegistered: isRegistered };
                
                // If player just registered, add their name and trophy count
                if (isRegistered && !existingData.name && existingPlayerData) {
                    updates.name = existingPlayerData.name;
                    updates.trophyCount = existingPlayerData.trophyCount;
                }
                
                await playerResultRef.update(updates);
            }
        }

        // Update season-specific results
        const seasonRef = database.ref(`players/${playerId}/seasons/${currentSeason}`);
        const seasonSnapshot = await seasonRef.once('value');
        if (!seasonSnapshot.exists()) {
            await seasonRef.set({
                totalPoints: 0
            });
        }

        // Update tournament tier stats for both season and overall
        const seasonTierRef = database.ref(`players/${playerId}/seasons/${currentSeason}/${tier}`);
        const overallTierRef = database.ref(`players/${playerId}/${tier}`);
        
        // Initialize tier stats if not exists
        await initializeTournamentStats(seasonTierRef);
        await initializeTournamentStats(overallTierRef);
        
        // Update placement stats
        const statField = getStatFieldForPlacement(placement);
        if (statField) {
            // Get current values
            const currentSeasonData = await seasonTierRef.once('value');
            const currentSeasonValue = currentSeasonData.val()?.[statField] || 0;
            
            const currentOverallData = await overallTierRef.once('value');
            const currentOverallValue = currentOverallData.val()?.[statField] || 0;
            
            // Update placement counters
            await Promise.all([
                seasonTierRef.update({
                    [statField]: currentSeasonValue + 1
                }),
                overallTierRef.update({
                    [statField]: currentOverallValue + 1
                })
            ]);
        }

        // Update points
        const currentTotalPoints = playerSnapshot.val()?.totalPoints || 0;
        const currentSeasonPoints = (await seasonRef.once('value')).val()?.totalPoints || 0;
        
        await Promise.all([
            playerResultRef.update({
                totalPoints: currentTotalPoints + points
            }),
            seasonRef.update({
                totalPoints: currentSeasonPoints + points
            })
        ]);

        const registrationStatus = isRegistered ? "registered" : "unregistered";
        console.log(`Recorded individual result for ${playerId} (${registrationStatus}): ${tier} ${placement}${getPlacementSuffix(placement)} place, +${points} points`);
    } catch (error) {
        console.error(`Error updating individual player result for ${playerId}:`, error);
        throw error;
    }
}

/**
 * Updates player's total points
 * @param {string} playerId - Normalized PlayFab ID
 * @param {number} points - Points to add
 */
async function updatePlayerPoints(playerId, points) {
    try {
        const playerLocation = await findPlayerLocation(playerId);
        if (!playerLocation) {
            console.warn(`Player ${playerId} not found for points update`);
            return;
        }

        const { clubId, playerKey } = playerLocation;
        const playerRef = database.ref(`clubs/${clubId}/players/${playerKey}`);
        const currentData = await playerRef.once('value');
        
        // Update overall total points (for backward compatibility)
        const currentTotalPoints = currentData.val()?.totalPoints || 0;
        
        // Update season-specific points
        const currentSeasonPoints = currentData.val()?.seasons?.[currentSeason]?.totalPoints || 0;
        
        const updates = {};
        updates.totalPoints = currentTotalPoints + points;
        updates[`seasons/${currentSeason}/totalPoints`] = currentSeasonPoints + points;

        await playerRef.update(updates);

        console.log(`Updated player ${playerId} points: +${points} (season ${currentSeason}: ${currentSeasonPoints + points}, total: ${currentTotalPoints + points})`);
    } catch (error) {
        console.error(`Error updating player points for ${playerId}:`, error);
        throw error;
    }
}

/**
 * Updates player's tournament statistics
 * @param {string} playerId - Normalized PlayFab ID
 * @param {string} tier - Tournament tier
 * @param {number} placement - Player placement
 */
async function updatePlayerTournamentStats(playerId, tier, placement) {
    if (tier === 'Unknown') return;

    try {
        const playerLocation = await findPlayerLocation(playerId);
        if (!playerLocation) {
            console.warn(`Player ${playerId} not found for tournament stats update`);
            return;
        }

        const { clubId, playerKey } = playerLocation;
        // Update season-specific stats
        const seasonTierRef = database.ref(`clubs/${clubId}/players/${playerKey}/seasons/${currentSeason}/${tier}`);
        // Update overall/total stats  
        const overallTierRef = database.ref(`clubs/${clubId}/players/${playerKey}/${tier}`);
        
        // Initialize tier stats if not exists for both season and overall
        await initializeTournamentStats(seasonTierRef);
        await initializeTournamentStats(overallTierRef);
        
        // Update appropriate placement counter
        const statField = getStatFieldForPlacement(placement);
        if (statField) {
            // Update season-specific stats
            const currentSeasonData = await seasonTierRef.once('value');
            const currentSeasonValue = currentSeasonData.val()?.[statField] || 0;
            
            // Update overall/total stats
            const currentOverallData = await overallTierRef.once('value');
            const currentOverallValue = currentOverallData.val()?.[statField] || 0;
            
            // Perform both updates
            await Promise.all([
                seasonTierRef.update({
                    [statField]: currentSeasonValue + 1
                }),
                overallTierRef.update({
                    [statField]: currentOverallValue + 1
                })
            ]);
            
            console.log(`Updated player ${playerId} ${tier} stats: ${statField} +1 (season ${currentSeason}: ${currentSeasonValue + 1}, total: ${currentOverallValue + 1})`);
        }
    } catch (error) {
        console.error(`Error updating player tournament stats for ${playerId}:`, error);
        throw error;
    }
}

/**
 * Finds the database location of a player by their PlayFab ID
 * @param {string} playerId - Normalized PlayFab ID
 * @returns {Object|null} Player location with clubId and playerKey
 */
async function findPlayerLocation(playerId) {
    try {
        const clubSnapshot = await database.ref('clubs').once('value');
        const clubs = clubSnapshot.val() || {};

        for (const [clubId, clubData] of Object.entries(clubs)) {
            const players = clubData.players || {};
            
            for (const [playerKey, playerData] of Object.entries(players)) {
                const normalizedId = playerData.playfabId?.toUpperCase().replace(/\s/g, '');
                if (normalizedId === playerId) {
                    return { clubId, playerKey };
                }
            }
        }
        
        return null;
    } catch (error) {
        console.error('Error finding player location:', error);
        return null;
    }
}

/**
 * Initializes tournament statistics structure if it doesn't exist
 * @param {Object} tierRef - Firebase reference to tournament tier
 */
async function initializeTournamentStats(tierRef) {
    try {
        const snapshot = await tierRef.once('value');
        if (!snapshot.exists()) {
            await tierRef.set({
                first: 0,
                second: 0,
                third: 0
            });
        }
    } catch (error) {
        console.error('Error initializing tournament stats:', error);
        throw error;
    }
}

/**
 * Maps placement number to database field name
 * @param {number} placement - Placement (1-4)
 * @returns {string|null} Database field name
 */
function getStatFieldForPlacement(placement) {
    const fieldMap = {
        1: 'first',
        2: 'second',
        3: 'third',
        4: 'third' // 4th place also counts as 3rd for bronze medal
    };
    
    return fieldMap[placement] || null;
}

// Main execution
if (require.main === module) {
    initializeTournamentFetcher()
        .then(() => {
            console.log('Tournament fetcher completed successfully');
            process.exit(0);
        })
        .catch((error) => {
            console.error('Tournament fetcher failed:', error);
            process.exit(1);
        });
}

module.exports = {
    initializeTournamentFetcher,
    fetchAndProcessTournaments,
    syncPlayFabPlayerData,
    getTierFromName,
    getPointsMapping
};
