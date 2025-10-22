const express = require("express");
const { Storage } = require("@google-cloud/storage");
const { MongoClient } = require("mongodb"); // Using JS Date, Timestamp not strictly needed here
const axios = require("axios");
const { bigQueryProcessor } = require("./bigQueryProcessor");

const app = express();

// Use JSON middleware to parse request bodies with increased size limit
app.use(express.json({ limit: "100mb" }));

const url = [
    "https://discord.com/api/webhooks/1411987240632193105/4nw0kE62xD8E-AA-Ajqwdwc0zAdrIjkNa1wlV9dFDV_OnT9AysY1CBj3fGL8vB_PuUVj",
    "https://discord.com/api/webhooks/1411987420169113650/i5NsjNfb0AHos5wemsTCtrVsZpja19ySzQdbonLZ20PhrSp9SZ0Y6HcVkAkwxmh8W1pr",
    "https://discord.com/api/webhooks/1411987552767836220/krPXdVwpFGhoIWrjaOa6P_CnehvNqDnmYMe3kEwEQcn10eLvBAE-ECn9qtrNVB1ImWBK",
    "https://discord.com/api/webhooks/1411987758511296593/Q1E2YZn1NBC9WOYZeq03LfA5EytC4pc-s_IOOBL2eAnIYESnl5khDCNewhFxmtaICJe-",
    "https://discord.com/api/webhooks/1411987872197771344/eCE-hsRttT7IoGKniS4HINUq1PlF5BKrwmeKgLn96HTD8dkH2DFF28iJmm78Sku2Fi3h"
];

const sendDiscordMessage = async (
    title = "Webhook Processor Logs",
    formattedMessage
) => {
    try {
        const randomIndex = Math.floor(Math.random() * url.length);
        const webhookUrl = url[randomIndex];
        const finalMessage = "```" + formattedMessage + "```";
        const response = await axios.post(
            webhookUrl,
            {
                content: `${finalMessage}`,
                username: title,
            },
            {
                headers: {
                    "Content-Type": "application/json",
                },
            }
        );
    } catch (error) {
        // console.error("Error sending Discord message:", error);
    }
};

const MONGODB_URI =
    "mongodb+srv://eazybe-backend:2u14oH9wSlBdOBXz@chat-backup-us-central1.ryikyh.mongodb.net/?retryWrites=true&w=majority&appName=chat-backup-us-central1";

const MONGODB_DB = "eazy-be";
const MONGO_CONVERSATION_COLLECTION = "backup_last_messages"; // Collection for summaries
const MONGO_MESSAGES_COLLECTION = "sent_waba_messages";
const MONGO_PUB_SUB_MESSAGES_COLLECTION = "pub_sub_messages";
const MONGO_BROADCAST_COLLECTION = "broadcasts";
const GCS_BUCKET = "meta-webhooks"; // Bucket for detailed logs
const PENDING_MESSAGE_WEBHOOK_URL =
    process.env.PENDING_MESSAGE_WEBHOOK_URL ||
    "https://pending-message-monitor-645252878991.us-east1.run.app/webhook";

// Validate essential configuration
if (
    !MONGODB_URI ||
    !MONGODB_DB ||
    !GCS_BUCKET ||
    !MONGO_CONVERSATION_COLLECTION
) {
    console.error(
        "FATAL ERROR: Missing one or more required configuration values: MONGODB_URI, MONGODB_DB, MONGO_CONVERSATION_COLLECTION, GCS_BUCKET. Check environment variables."
    );
    process.exit(1); // Exit if essential config is missing
}

// --- Initialize Clients ---

const storage = new Storage({
    retryOptions: {
        autoRetry: true,
        maxRetries: 5,
        retryDelayMultiplier: 2,
        totalTimeout: 120000,
        maxRetryDelay: 30000,
    },
});
const bucket = storage.bucket(GCS_BUCKET);
const mongoClient = new MongoClient(MONGODB_URI);
const mongoBroadcastClient = new MongoClient(
    "mongodb+srv://eazybe_chatter:fGnTERHDIXvjIuFi@cluster0.3hm3sit.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
);
let mongoConversationCollection;
let mongoSentMsgCollection;
let mongoPubSubMessagesCollection;
let mongoBroadcastCollection;

// File locking mechanism to prevent concurrent writes
const fileLocks = new Map();
const lockTimeouts = new Map();

// Message sequence tracking for missing message detection
const sequenceTracker = new Map(); // chatter -> { lastNumber, lastTimestamp, missingNumbers }

// Function to track message sequences and detect missing messages
const trackMessageSequence = async (chatter, messages) => {
    const numberedMessages = messages.filter(msg => {
        const match = msg.Message.match(/^(\w+)\s+(\d+)$/);
        return match !== null;
    });

    if (numberedMessages.length === 0) return;

    // Sort by number
    const sortedMessages = numberedMessages.map(msg => {
        const match = msg.Message.match(/^(\w+)\s+(\d+)$/);
        return {
            number: parseInt(match[2]),
            message: msg.Message,
            timestamp: msg.Datetime,
            messageId: msg.MessageId
        };
    }).sort((a, b) => a.number - b.number);

    const sequenceName = sortedMessages[0].message.split(' ')[0];
    const currentNumbers = sortedMessages.map(m => m.number);
    const minNumber = Math.min(...currentNumbers);
    const maxNumber = Math.max(...currentNumbers);

    // Get or create tracker for this chatter
    let tracker = sequenceTracker.get(chatter) || {
        sequences: new Map(),
        lastUpdate: Date.now()
    };

    // Get or create sequence tracker
    let sequenceData = tracker.sequences.get(sequenceName) || {
        lastNumber: 0,
        lastTimestamp: 0,
        missingNumbers: new Set(),
        receivedNumbers: new Set(),
        totalReceived: 0
    };

    // Update received numbers
    currentNumbers.forEach(num => {
        sequenceData.receivedNumbers.add(num);
        sequenceData.totalReceived++;
    });

    // Check for missing numbers in current batch
    const expectedNumbers = Array.from({ length: maxNumber - minNumber + 1 }, (_, i) => minNumber + i);
    const missingInBatch = expectedNumbers.filter(n => !currentNumbers.includes(n));

    if (missingInBatch.length > 0) {
        console.log(`🚨 MISSING MESSAGES DETECTED in current batch for ${chatter}:`);
        console.log(`  Sequence: ${sequenceName}`);
        console.log(`  Missing: ${missingInBatch.join(', ')}`);
        console.log(`  Missing messages: ${missingInBatch.map(n => `${sequenceName} ${n}`).join(', ')}`);

        // Add to missing numbers set
        missingInBatch.forEach(num => sequenceData.missingNumbers.add(num));

        // Send critical alert
        await sendDiscordMessage(
            "CRITICAL: Missing Messages in Current Batch",
            `🚨 MISSING MESSAGES DETECTED\nChatter: ${chatter}\nSequence: ${sequenceName}\nExpected: ${minNumber} to ${maxNumber}\nMissing: ${missingInBatch.join(', ')}\nMissing Messages: ${missingInBatch.map(n => `${sequenceName} ${n}`).join(', ')}\nReceived: ${currentNumbers.join(', ')}\nTime: ${new Date().toISOString()}`
        );
    }

    // Check for gaps with previous data
    if (sequenceData.lastNumber > 0) {
        const gapStart = sequenceData.lastNumber + 1;
        const gapEnd = minNumber - 1;

        if (gapEnd >= gapStart) {
            const gapNumbers = Array.from({ length: gapEnd - gapStart + 1 }, (_, i) => gapStart + i);
            console.log(`🚨 GAP DETECTED between batches for ${chatter}:`);
            console.log(`  Sequence: ${sequenceName}`);
            console.log(`  Gap: ${gapNumbers.join(', ')}`);
            console.log(`  Gap messages: ${gapNumbers.map(n => `${sequenceName} ${n}`).join(', ')}`);

            // Add gap numbers to missing set
            gapNumbers.forEach(num => sequenceData.missingNumbers.add(num));

            // Send gap alert
            await sendDiscordMessage(
                "CRITICAL: Message Gap Detected",
                `🚨 MESSAGE GAP DETECTED\nChatter: ${chatter}\nSequence: ${sequenceName}\nGap: ${gapNumbers.join(', ')}\nGap Messages: ${gapNumbers.map(n => `${sequenceName} ${n}`).join(', ')}\nPrevious: ${sequenceName} ${sequenceData.lastNumber}\nCurrent: ${sequenceName} ${minNumber}\nTime: ${new Date().toISOString()}`
            );
        }
    }

    // Update tracker
    sequenceData.lastNumber = maxNumber;
    sequenceData.lastTimestamp = Math.max(...sortedMessages.map(m => m.timestamp));
    tracker.sequences.set(sequenceName, sequenceData);
    tracker.lastUpdate = Date.now();
    sequenceTracker.set(chatter, tracker);

    // Log summary
    console.log(`📊 Sequence tracking summary for ${chatter}:`);
    console.log(`  Sequence: ${sequenceName}`);
    console.log(`  Range: ${minNumber} to ${maxNumber}`);
    console.log(`  Received: ${currentNumbers.length} messages`);
    console.log(`  Missing in batch: ${missingInBatch.length}`);
    console.log(`  Total missing: ${sequenceData.missingNumbers.size}`);
    console.log(`  Total received: ${sequenceData.totalReceived}`);

    return {
        sequenceName,
        minNumber,
        maxNumber,
        received: currentNumbers.length,
        missingInBatch: missingInBatch.length,
        totalMissing: sequenceData.missingNumbers.size,
        totalReceived: sequenceData.totalReceived
    };
};

// Lock timeout (5 minutes)
// FIXED: Better file locking with longer timeout
const LOCK_TIMEOUT = 10 * 60 * 1000; // 10 minutes for large files

const acquireFileLock = async (filePath) => {
    const lockKey = `lock_${filePath}`;
    const startTime = Date.now();
    const maxWaitTime = 60000; // 60 seconds max wait

    while (fileLocks.has(lockKey)) {
        if (Date.now() - startTime > maxWaitTime) {
            const waitingTime = Date.now() - startTime;
            console.error(`🚨 File lock timeout for ${filePath} after ${waitingTime}ms`);
            throw new Error(`File lock timeout for ${filePath} after ${waitingTime}ms`);
        }
        console.log(`⏳ Waiting for file lock: ${filePath} (${Date.now() - startTime}ms)`);
        await new Promise(resolve => setTimeout(resolve, 500)); // Wait 500ms
    }

    fileLocks.set(lockKey, Date.now());

    // Set timeout to automatically release lock
    const timeoutId = setTimeout(() => {
        if (fileLocks.has(lockKey)) {
            fileLocks.delete(lockKey);
            lockTimeouts.delete(lockKey);
            console.warn(`⚠️ File lock automatically released for ${filePath} due to timeout`);
        }
    }, LOCK_TIMEOUT);

    lockTimeouts.set(lockKey, timeoutId);
    console.log(`🔒 File lock acquired for ${filePath}`);
    return lockKey;
};

const releaseFileLock = (lockKey) => {
    if (fileLocks.has(lockKey)) {
        fileLocks.delete(lockKey);
        const timeoutId = lockTimeouts.get(lockKey);
        if (timeoutId) {
            clearTimeout(timeoutId);
            lockTimeouts.delete(lockKey);
        }
        console.log(`File lock released for ${lockKey}`);
    }
};

// Enhanced file save with retry and locking
const saveFileWithLock = async (file, content, options = {}, maxRetries = 3) => {
    const filePath = file.name;
    let lockKey = null;

    try {
        // Acquire file lock
        lockKey = await acquireFileLock(filePath);

        // Retry logic with exponential backoff
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                await file.save(content, {
                    contentType: "application/json",
                    resumable: attempt > 1,
                    ...options
                });

                console.log(`✅ File saved successfully: ${filePath} (attempt ${attempt})`);
                return true;

            } catch (error) {
                console.error(`❌ File save attempt ${attempt} failed for ${filePath}:`, error.message);

                if (attempt === maxRetries) {
                    throw error;
                }

                // Exponential backoff: 1s, 2s, 4s
                const delay = Math.pow(2, attempt - 1) * 1000;
                console.log(`⏳ Retrying in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }

    } finally {
        // Always release the lock
        if (lockKey) {
            releaseFileLock(lockKey);
        }
    }
};

// Enhanced file processing with proper message merging
const processFileWithLock = async (file, eventData, dateAccChats, chatKey) => {
    const filePath = file.name;
    let lockKey = null;

    try {
        // Acquire file lock
        lockKey = await acquireFileLock(filePath);

        // Download file with retry
        const buffer = await downloadFileWithRetry(file);
        const fileContent = buffer.toString("utf8");
        let existingContent = JSON.parse(fileContent);

        // Merge name mappings
        for (let name in eventData.nameMapping) {
            if (!existingContent.nameMapping[name]) {
                existingContent.nameMapping[name] = "";
            }
            if (eventData.nameMapping[name]) {
                existingContent.nameMapping[name] = eventData.nameMapping[name];
            }
        }

        // Process and merge messages for each chatter
        for (let chatter in dateAccChats[chatKey]) {

            // Initialize chatter array if it doesn't exist
            if (!existingContent.chatters.hasOwnProperty(chatter)) {
                existingContent.chatters[chatter] = [];
            } else {
                console.log(`📁 Existing chatter array for ${chatter} has ${existingContent.chatters[chatter].length} messages`);
            }

            // Get existing messages for this chatter
            const existingMessages = existingContent.chatters[chatter];

            // ENHANCED DEDUPLICATION: Only filter true duplicates (same MessageId AND same content AND same timestamp)
            const existingMessageMap = new Map();

            // Create a map of existing messages for comprehensive comparison
            existingMessages.forEach(msg => {
                const key = `${msg.MessageId}_${msg.Datetime}_${msg.Message?.substring(0, 50)}`;
                existingMessageMap.set(key, msg);
            });

            console.log(`🔍 Processing ${dateAccChats[chatKey][chatter].length} messages for ${chatter}`);
            console.log(`📋 Existing messages: ${existingMessages.length}`);

            // Filter out only true duplicates
            const newMessages = dateAccChats[chatKey][chatter].filter(newMsg => {
                const newKey = `${newMsg.MessageId}_${newMsg.Datetime}_${newMsg.Message?.substring(0, 50)}`;
                const isDuplicate = existingMessageMap.has(newKey);

                if (isDuplicate) {
                    console.log(`⚠️ Skipping duplicate message: ${newMsg.MessageId} (${newMsg.Message}) - ${new Date(newMsg.Datetime).toISOString()}`);
                    return false;
                }
                return true;
            });

            if (newMessages.length > 0) {
                console.log(`  Adding ${newMessages.length} new messages to ${chatter} (${dateAccChats[chatKey][chatter].length - newMessages.length} duplicates skipped)`);

                // Log each new message being added
                newMessages.forEach((msg, index) => {
                    console.log(`  📝 ${index + 1}. ${msg.Message} (${msg.MessageId}) - ${new Date(msg.Datetime).toISOString()}`);
                });

                // Append new messages after existing messages
                existingContent.chatters[chatter] = [
                    ...existingContent.chatters[chatter],
                    ...newMessages,
                ];

                // Sort all messages by timestamp to maintain proper sequence
                existingContent.chatters[chatter] = existingContent.chatters[chatter].sort((a, b) => a.Datetime - b.Datetime);

                // Validate no messages were lost
                const finalCount = existingContent.chatters[chatter].length;
                const expectedCount = existingMessages.length + newMessages.length;
                if (finalCount !== expectedCount) {
                    console.error(`🚨 MESSAGE COUNT MISMATCH for ${chatter}: expected ${expectedCount}, got ${finalCount}`);
                }
            } else {
                console.log(`  No new messages to add for ${chatter} (all were duplicates)`);
            }
        }

        // Save updated content
        await file.save(JSON.stringify(existingContent, null, 2), {
            contentType: "application/json"
        });

        console.log(`✅ File updated successfully: ${filePath}`);
        return true;

    } catch (error) {
        console.error(`❌ Error processing file ${filePath}:`, error);
        throw error;
    } finally {
        // Always release the lock
        if (lockKey) {
            releaseFileLock(lockKey);
        }
    }
};

// Enhanced file download with retry
const downloadFileWithRetry = async (file, maxRetries = 3) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const [buffer] = await file.download();
            return buffer;
        } catch (error) {
            console.error(`❌ File download attempt ${attempt} failed for ${file.name}:`, error.message);

            if (attempt === maxRetries) {
                throw error;
            }

            const delay = Math.pow(2, attempt - 1) * 1000;
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
};

// --- Connect to MongoDB ---
async function connectToMongo() {
    try {
        // Connect to main MongoDB for regular collections
        if (mongoClient?.topology && mongoClient.topology.isConnected()) {
            console.log("Main MongoDB client already connected");
        } else {
            console.log("Connecting to main MongoDB...");
            await mongoClient.connect({
                maxPoolSize: 100,
                minPoolSize: 10,
                serverSelectionTimeoutMS: 5000,
                socketTimeoutMS: 45000,
            });
            console.log("Main MongoDB connected successfully");
        }

        // Connect to broadcast MongoDB
        if (
            mongoBroadcastClient?.topology &&
            mongoBroadcastClient.topology.isConnected()
        ) {
            console.log("Broadcast MongoDB client already connected");
        } else {
            console.log("Connecting to broadcast MongoDB...");
            await mongoBroadcastClient.connect({
                maxPoolSize: 100,
                minPoolSize: 10,
                serverSelectionTimeoutMS: 5000,
                socketTimeoutMS: 45000,
            });
            console.log("Broadcast MongoDB connected successfully");
        }

        const db = mongoClient.db(MONGODB_DB);
        const broadcastDb = mongoBroadcastClient.db("eazy-be"); // Using same database name for broadcast

        mongoConversationCollection = db.collection(
            MONGO_CONVERSATION_COLLECTION
        );
        mongoSentMsgCollection = db.collection(MONGO_MESSAGES_COLLECTION);
        mongoPubSubMessagesCollection = db.collection(
            MONGO_PUB_SUB_MESSAGES_COLLECTION
        );
        mongoBroadcastCollection = db.collection(MONGO_BROADCAST_COLLECTION);

        console.log(
            `Connected to main MongoDB, using database '${MONGODB_DB}' and collection '${MONGO_CONVERSATION_COLLECTION}'.`
        );
        console.log(
            `Connected to broadcast MongoDB, using collection '${MONGO_BROADCAST_COLLECTION}'.`
        );

        await mongoPubSubMessagesCollection.createIndex({ messageId: 1 });

        await mongoConversationCollection.createIndex(
            { from: 1, to: 1 },
            { unique: true }
        );
        console.log(
            `Ensured unique index exists on ${MONGO_CONVERSATION_COLLECTION} collection (from, to).`
        );

        // Set up change stream monitor for incoming messages
        // await setupChangeStreamMonitor();
    } catch (err) {
        console.error(
            "FATAL ERROR: MongoDB connection or index creation failed:",
            err
        );
        process.exit(1);
    }
}
connectToMongo();

// Two-step fallback query for broadcast records with webhook race condition handling
const findBroadcastRecordWithFallback = async (whatsappMessageId, recipientPhoneNumber) => {
    try {
        // Step 1: Primary query - try to find by whatsapp_message_id
        let record = await mongoBroadcastCollection.findOne({
            whatsapp_message_id: whatsappMessageId
        });

        if (record) {
            console.log(`✅ Found broadcast record by message ID: ${whatsappMessageId}`);
            return record;
        }

        // Step 2: Fallback query - find by recipient phone number and pending status
        console.log(`⚠️ Message ID not found, trying fallback query for phone: ${recipientPhoneNumber}`);
        record = await mongoBroadcastCollection.findOne({
            recipient_phone_number: recipientPhoneNumber,
            processing_status: "PENDING_SEND"
        }, {
            sort: { created_at: -1 } // Get the most recent one
        });

        if (record) {
            console.log(`✅ Found broadcast record by fallback query, updating with message ID: ${whatsappMessageId}`);

            // Update the record with the actual message ID
            await mongoBroadcastCollection.updateOne(
                { _id: record._id },
                {
                    $set: {
                        whatsapp_message_id: whatsappMessageId,
                        processing_status: "PENDING_WEBHOOK",
                        updated_at: new Date()
                    }
                }
            );

            // Return the updated record
            return {
                ...record,
                whatsapp_message_id: whatsappMessageId,
                processing_status: "PENDING_WEBHOOK"
            };
        }

        console.log(`❌ No broadcast record found for message ID: ${whatsappMessageId} or phone: ${recipientPhoneNumber}`);
        return null;

    } catch (error) {
        console.error(`Error finding broadcast record for webhook:`, error);
        return null;
    }
};

// Retry helper for template code lookup with exponential backoff
// Delay progression: 1.5s, 4.5s, 13.5s, 40.5s (max delay: 40.5s, total: ~59.25s)
const findTemplateCodeWithRetry = async (
    msgId,
    {
        includeSentWabaFallback = false,
        attempts = 4,
        initialDelayMs = 1500,
        factor = 3,
    } = {}
) => {
    let currentDelay = initialDelayMs;
    for (let attempt = 0; attempt < attempts; attempt++) {
        try {
            // Try broadcast collection first with fallback logic
            if (mongoBroadcastCollection) {
                // For template lookup, we need to extract recipient phone number from context
                // This will be passed from the calling function
                const broadcastRecord = await mongoBroadcastCollection.findOne({
                    whatsapp_message_id: msgId,
                });
                if (broadcastRecord && broadcastRecord.template_id) {
                    return {
                        template_code: broadcastRecord.template_id,
                        source: "broadcast_collection",
                    };
                }
            }

            // Optional fallback to sent messages collection
            if (includeSentWabaFallback && mongoSentMsgCollection) {
                const sentWabaRecord = await mongoSentMsgCollection.findOne({
                    msgId: msgId,
                });
                if (
                    sentWabaRecord &&
                    sentWabaRecord.template_id &&
                    sentWabaRecord.template_id !== "unknown_template"
                ) {
                    return {
                        template_code: sentWabaRecord.template_id,
                        source: "sentWabaMesg_collection",
                    };
                }
            }
            sendDiscordMessage("Template Lookup Error - Retry", `🚨 Template lookup failed for message ${msgId} after ${attempts} attempts. Error: ${err.message}`);
        } catch (err) {
            console.error(
                `Retry attempt ${attempt + 1
                } failed during template lookup for ${msgId}:`,
                err
            );
        }

        // If not found and more attempts remain, wait with backoff
        if (attempt < attempts - 1) {
            await new Promise((resolve) => setTimeout(resolve, currentDelay));
            currentDelay = currentDelay * factor;
        }
    }
    return null;
};

// Enhanced template lookup with fallback logic for webhook processing
const findTemplateCodeWithFallback = async (msgId, recipientPhoneNumber, includeSentWabaFallback = false) => {
    try {
        // First try the fallback broadcast record lookup
        const broadcastRecord = await findBroadcastRecordWithFallback(msgId, recipientPhoneNumber);

        if (broadcastRecord && broadcastRecord.template_id) {
            console.log(`✅ Template found via fallback broadcast lookup: ${broadcastRecord.template_id}`);
            return {
                template_code: broadcastRecord.template_id,
                source: "broadcast_collection_fallback",
                broadcast_record: broadcastRecord
            };
        }

        // If no broadcast record found, try the original retry logic
        const retryResult = await findTemplateCodeWithRetry(msgId, {
            includeSentWabaFallback: includeSentWabaFallback,
            attempts: 4,
            initialDelayMs: 1500,
            factor: 3,
        });

        if (retryResult && retryResult.template_code) {
            console.log(`✅ Template found via retry logic: ${retryResult.template_code}`);
            return retryResult;
        }

        console.log(`❌ No template found for message ID: ${msgId}`);
        return null;

    } catch (error) {
        console.error(`Error in findTemplateCodeWithFallback:`, error);
        return null;
    }
};

// Debug function to help track message lookup issues
async function debugMessageLookup(msgId, from, to, webhookData = null) {
    try {
        console.log(`🔍 Debugging message lookup for: ${msgId}`);

        // Check if message exists in sent messages collection
        const sentMsg = await mongoSentMsgCollection.findOne({ msgId: msgId });
        if (sentMsg) {
            console.log(`✅ Message found in sent messages:`, {
                msgId: sentMsg.msgId,
                type: sentMsg.type,
                last_message: sentMsg.last_message,
                createdAt: sentMsg.createdAt,
            });
            return sentMsg;
        }

        // Check recent messages by phone numbers
        const recentMessages = await mongoSentMsgCollection
            .find({
                from: from,
                to: to,
                createdAt: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) },
            })
            .sort({ createdAt: -1 })
            .limit(5)
            .toArray();

        if (recentMessages.length > 0) {
            console.log(
                `📱 Found ${recentMessages.length} recent messages:`,
                recentMessages.map((m) => ({
                    msgId: m.msgId,
                    type: m.type,
                    last_message: m.last_message,
                    createdAt: m.createdAt,
                }))
            );
        } else {
            console.log(`❌ No recent messages found for ${from} -> ${to}`);
        }

        // Analyze webhook data for template indicators
        if (webhookData) {
            console.log(`🔍 Webhook data analysis:`, {
                conversation_origin: webhookData.conversation?.origin?.type,
                pricing_category: webhookData.pricing?.category,
                is_marketing:
                    webhookData.conversation?.origin?.type === "marketing",
                suggested_type:
                    webhookData.conversation?.origin?.type === "marketing"
                        ? "template"
                        : "text",
            });
        }

        return null;
    } catch (error) {
        console.error(`Error in debugMessageLookup:`, error);
        return null;
    }
}

// Extracts event data, handling Pub/Sub envelope if present
function extractEventData(req) {
    let eventData = req.body;
    if (req.body && req.body.message && req.body.message.data) {
        try {
            const decodedData = Buffer.from(
                req.body.message.data,
                "base64"
            ).toString("utf8");
            eventData = JSON.parse(decodedData);
            console.log("Decoded event data from Pub/Sub envelope.");
        } catch (err) {
            console.warn(
                "Failed to decode or parse Pub/Sub message data:",
                err
            );
            eventData = { rawBody: req.body, parseError: err.message };
        }
    }
    return eventData;
}

async function createLastMessages(
    orgId,
    backupUser,
    chatters,
    nameMap,
    profileImageMap
) {
    if (typeof chatters !== "object") return null;

    await connectToMongo();

    // Extract all receiver numbers at once
    const toChattersArray = Object.keys(chatters).map(
        (chatter) => chatter.split("@")[0]
    );

    // Fetch all relevant last messages in a single query
    const allLastMessages = await mongoConversationCollection
        .find({
            from: backupUser,
            to: { $in: toChattersArray },
        })
        .toArray();

    // Create a map for faster lookups
    const lastMessagesMap = Object.fromEntries(
        allLastMessages.map((msg) => [msg.to, msg])
    );

    // Prepare bulk operations
    const bulkOps = [];

    // Process each chatter
    for (const chatter in chatters) {
        const chatterArr = chatter.split("@");
        if (chatterArr.length !== 2) continue;

        const receiverNumber = chatterArr[0];
        const chats = chatters[chatter][chatters[chatter].length - 1];
        const receiverName = nameMap[chatter] || "";

        if (!mongoConversationCollection) continue;

        // Check if we already have this conversation
        const getLastMessage = lastMessagesMap[receiverNumber];

        if (!getLastMessage) {
            // Create new conversation document
            const data = {
                from: backupUser,
                to: receiverNumber,
                toName: receiverName,
                Photo: profileImageMap ? profileImageMap[chatter] || "" : "",
                orgId: orgId,
                last_message: chats.Message,
                last_message_time: chats?.Datetime,
                last_message_type:
                    chats?.File === null || chats?.File === "null"
                        ? "text"
                        : "file",
                last_message_wamid: chats.MessageId,
                last_message_direction: chats?.Direction,
                last_message_unreadCount: chats?.unreadCount || 0,
                chats: [],
                createdAt: new Date(),
                updatedAt: new Date(),
            };

            // Add to bulk operations
            bulkOps.push({
                insertOne: { document: data },
            });

            // Update local cache
            lastMessagesMap[receiverNumber] = data;
        }

        // Get the chats array (either from existing document or newly created one)
        const chatsArray = lastMessagesMap[receiverNumber].chats || [];

        // Prepare update operation
        const update = {
            $set: {
                toName: receiverName,
                // Photo: profileImageMap ? (profileImageMap[chatter] || "") : "",
                last_message: chats.Message,
                last_message_time: chats?.Datetime,
                last_message_type:
                    chats?.File === null || chats?.File === "null"
                        ? "text"
                        : "file",
                last_message_wamid: chats.MessageId,
                last_message_direction: chats?.Direction,
                last_message_unreadCount: chats?.unreadCount || 0,
                orgId: orgId,
                updatedAt: new Date(),
                ...(profileImageMap && profileImageMap[chatter]
                    ? { Photo: profileImageMap[chatter] || "" }
                    : {}),
            },
        };

        // Process all chats for this chatter
        const existingDates = new Set(chatsArray.map((item) => item.date));

        for (const chat of chatters[chatter]) {
            if (!existingDates.has(chat.Date)) {
                const filePath = `${orgId}/${chat.Date.split("-")[0]}/${chat.Date.split("-")[1]
                    }/${chat.Date.split("-")[2]}/${backupUser}.json`;

                chatsArray.push({
                    date: chat.Date,
                    filePath: filePath,
                });

                existingDates.add(chat.Date);
            }
        }

        update.$set.chats = chatsArray;

        // Add to bulk operations if it's an existing document
        if (getLastMessage) {
            bulkOps.push({
                updateOne: {
                    filter: { from: backupUser, to: receiverNumber },
                    update: update,
                },
            });
        }
    }

    // Execute all operations in bulk if there are any
    if (bulkOps.length > 0) {
        await mongoConversationCollection.bulkWrite(bulkOps);
    }

    return null;
}

// Check for send payload format
async function checkForSendFormat(eventData) {
    if (
        !eventData?.object ||
        eventData.object !== "whatsapp_business_account" ||
        !eventData.entry ||
        !Array.isArray(eventData.entry) ||
        eventData.entry.length === 0
    ) {
        return null;
    }

    const entry = eventData.entry[0];
    if (
        !entry.changes ||
        !Array.isArray(entry.changes) ||
        entry.changes.length === 0
    ) {
        return null;
    }

    const change = entry.changes[0];
    if (
        !change.value ||
        !change.value.statuses ||
        !Array.isArray(change.value.statuses) ||
        change.value.statuses.length === 0
    ) {
        return null;
    }

    const status = change.value.statuses[0];
    const metadata = change.value.metadata;

    if (
        !status?.id ||
        !status?.status ||
        !status?.recipient_id ||
        !metadata?.display_phone_number
    ) {
        return false;
    }

    let msgId = status.id;
    let msgTimestamp = status.timestamp
        ? parseInt(status.timestamp) * 1000
        : Date.now();
    const direction = "OUTGOING";

    let from = metadata.display_phone_number;
    let to = status.recipient_id;

    let sentMsg = await mongoSentMsgCollection.findOne({
        msgId: msgId,
    });

    if (!sentMsg) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
        sentMsg = await mongoSentMsgCollection.findOne({
            msgId: msgId,
        });

        // If we found a message but it's a template without proper template string, update it
        if (
            sentMsg &&
            sentMsg.type === "template" &&
            (!sentMsg.last_message ||
                !sentMsg.last_message.startsWith("%%%template%%%"))
        ) {
            try {
                // Try to find template code from broadcast collection using fallback logic
                const broadcastRecord = await findBroadcastRecordWithFallback(msgId, to);

                if (broadcastRecord && broadcastRecord.template_id) {
                    const updateResult = await mongoSentMsgCollection.updateOne(
                        { msgId: msgId },
                        {
                            $set: {
                                last_message: `%%%template%%% ${broadcastRecord.template_id}`,
                                template_id: broadcastRecord.template_id,
                                updatedAt: new Date(),
                            },
                        }
                    );
                    if (updateResult.modifiedCount > 0) {
                        sentMsg.last_message = `%%%template%%% ${broadcastRecord.template_id}`;
                        sentMsg.template_id = broadcastRecord.template_id;
                        console.log(
                            `✅ Updated existing template message with template code: ${broadcastRecord.template_id}`
                        );
                    }
                } else {
                    // Update with unknown template if no code found
                    const updateResult = await mongoSentMsgCollection.updateOne(
                        { msgId: msgId },
                        {
                            $set: {
                                last_message: `%%%template%%% unknown_template`,
                                template_id: "unknown_template",
                                updatedAt: new Date(),
                            },
                        }
                    );
                    if (updateResult.modifiedCount > 0) {
                        sentMsg.last_message = `%%%template%%% unknown_template`;
                        sentMsg.template_id = "unknown_template";
                        console.log(
                            `✅ Updated existing template message with unknown template`
                        );
                    }
                }
            } catch (error) {
                console.error(
                    "Error updating existing template message:",
                    error
                );
            }
        }

        // After any updates, refresh the sentMsg object to ensure we have the latest data
        if (sentMsg) {
            const refreshedMsg = await mongoSentMsgCollection.findOne({
                msgId: msgId,
            });
            if (refreshedMsg) {
                sentMsg = refreshedMsg;
                console.log(`🔄 Refreshed sentMsg object with latest data:`, {
                    msgId: sentMsg.msgId,
                    type: sentMsg.type,
                    last_message: sentMsg.last_message,
                    template_id: sentMsg.template_id,
                });
            }
        }

        if (!sentMsg) {
            // Try to find message by from and to phone numbers
            console.log(
                `Message not found by ID ${msgId}, trying to find by phone numbers: ${from} -> ${to}`
            );

            // Use debug function to get more insights
            // await debugMessageLookup(msgId, from, to, status);

            // Log the webhook status data for debugging
            console.log(`Webhook status data for message ${msgId}:`, {
                status: status.status,
                conversation_origin: status.conversation?.origin?.type,
                pricing_category: status.pricing?.category,
                is_marketing: status.conversation?.origin?.type === "marketing",
            });

            // Try multiple search strategies
            const searchStrategies = [
                { from: from, to: to },
                {
                    from: from,
                    to: to,
                    createdAt: {
                        $gte: new Date(Date.now() - 24 * 60 * 60 * 1000),
                    },
                }, // Last 24 hours
                {
                    from: from,
                    to: to,
                    createdAt: {
                        $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
                    },
                }, // Last 7 days
            ];

            for (let i = 0; i < searchStrategies.length; i++) {
                sentMsg = await mongoSentMsgCollection.findOne(
                    searchStrategies[i]
                );
                if (sentMsg) {
                    console.log(
                        `Found message using search strategy ${i + 1}:`,
                        {
                            msgId: sentMsg.msgId,
                            type: sentMsg.type,
                            createdAt: sentMsg.createdAt,
                            payload: sentMsg.payload || sentMsg.last_message,
                            searchStrategy: searchStrategies[i],
                        }
                    );
                    break;
                }
            }

            if (sentMsg) {
                console.log(`Found message by phone numbers:`, {
                    msgId: sentMsg.msgId,
                    type: sentMsg.type,
                    createdAt: sentMsg.createdAt,
                    payload: sentMsg.payload || sentMsg.last_message,
                });
            } else {
                console.log(
                    `No message found by phone numbers either, extracting from webhook event`
                );

                // Try to extract meaningful information from the webhook event
                // Note: Status update webhooks don't contain the actual message content
                // They only contain delivery status, conversation metadata, and pricing info
                // The real message content would be in the original message webhook (not status update)
                let webhookMessage = ``;

                // Add more context if available
                if (status.conversation?.origin?.type) {
                    webhookMessage += ` - ${status.conversation.origin.type}`;
                }

                // Check if this is a template message based on conversation origin
                if (status.conversation?.origin?.type === "marketing") {
                    try {
                        const result = await findTemplateCodeWithFallback(
                            msgId,
                            to, // recipient phone number for fallback query
                            false // don't include sentWaba fallback
                        );

                        if (result && result.template_code) {
                            webhookMessage = `%%%template%%% ${result.template_code}`;
                            console.log(
                                `Template code found for webhook message with retry: ${result.template_code}`
                            );

                            try {
                                await mongoSentMsgCollection.insertOne({
                                    msgId: msgId,
                                    from: from,
                                    to: to,
                                    type: "template",
                                    last_message: webhookMessage,
                                    isBroadcast: true,
                                    createdAt: new Date(),
                                    updatedAt: new Date(),
                                    template_id: result.template_code,
                                    pricing: status.pricing || null,
                                    conversation_origin:
                                        status.conversation?.origin?.type,
                                });
                                console.log(
                                    `✅ Created new template message record in MongoDB: ${msgId}`
                                );
                            } catch (insertError) {
                                console.error(
                                    `Error creating template message record:`,
                                    insertError
                                );
                            }
                        } else {
                            // Still no record after retries; create an unknown template record
                            webhookMessage = `%%%template%%% unknown_template`;
                            console.log(
                                `No template code found after retries, created unknown template message`
                            );

                            try {
                                await mongoSentMsgCollection.insertOne({
                                    msgId: msgId,
                                    from: from,
                                    to: to,
                                    type: "template",
                                    last_message: webhookMessage,
                                    isBroadcast: true,
                                    createdAt: new Date(),
                                    updatedAt: new Date(),
                                    template_id: "unknown_template",
                                    pricing: status.pricing || null,
                                    conversation_origin:
                                        status.conversation?.origin?.type,
                                });
                                console.log(
                                    `✅ Created unknown template message record in MongoDB: ${msgId}`
                                );
                            } catch (insertError) {
                                console.error(
                                    `Error creating unknown template message record:`,
                                    insertError
                                );
                            }
                        }
                    } catch (error) {
                        console.error(
                            "Error looking up template code with retry:",
                            error
                        );
                        // Even on error, create a template record
                        webhookMessage = `%%%template%%% error_template`;

                        try {
                            await mongoSentMsgCollection.insertOne({
                                msgId: msgId,
                                from: from,
                                to: to,
                                type: "template",
                                last_message: webhookMessage,
                                isBroadcast: true,
                                createdAt: new Date(),
                                updatedAt: new Date(),
                                template_id: "error_template",
                                pricing: status.pricing || null,
                                conversation_origin:
                                    status.conversation?.origin?.type,
                                error: error.message,
                            });
                            console.log(
                                `✅ Created error template message record in MongoDB: ${msgId}`
                            );
                        } catch (insertError) {
                            console.error(
                                `Error creating error template message record:`,
                                insertError
                            );
                        }
                    }
                }

                console.log(`Webhook event data available:`, {
                    status: status.status,
                    conversation: status.conversation,
                    pricing: status.pricing,
                    metadata: metadata,
                });

                sentMsg = {
                    type:
                        status.conversation?.origin?.type === "marketing"
                            ? "template"
                            : "text",
                    last_message: webhookMessage,
                    isBroadcast: false,
                };

                console.log(`Created fallback sentMsg with webhookMessage:`, {
                    type: sentMsg.type,
                    last_message: sentMsg.last_message,
                    conversation_origin: status.conversation?.origin?.type,
                    is_marketing:
                        status.conversation?.origin?.type === "marketing",
                });
            }
        }
    }

    if (!from || !to || !msgId || !msgTimestamp) {
        return NaN;
    }

    let statusValue = status.status;
    if (statusValue !== "delivered" && statusValue !== "sent") {
        return null;
    }
    let uid = null;
    let org_id = null;
    try {
        const response = await axios.post(
            "https://dev.eazybe.com/v2/waba/get-embedded-signup-by-phone-number",
            { phone_number: from },
            {
                headers: {
                    "Content-Type": "application/json",
                    "private-key": "123456789",
                },
            }
        );

        if (response.data?.error) {
            return null;
        }
        uid = response.data?.data?.workapace_id;
        org_id = response.data?.data?.org_id;
    } catch (err) {
        console.log(
            "---------------ERROR START while fetching org id from API-------------"
        );
        console.log(err);
        console.log(
            "---------------ERROR END while fetching org id from API-------------"
        );
    }

    if (!org_id) {
        console.log("⚠️ No org_id found, using default for testing");
        org_id = "missing_org_id"; // Use default org_id for testing
    }

    if (!uid) {
        console.log("⚠️ No uid found, using default for testing");
        uid = "missing_uid"; // Use default workspace_id for testing
    }

    let isBroadcast = false;
    // Set isBroadcast to true for template messages
    if (sentMsg && sentMsg.type === "template") {
        isBroadcast = true;
        console.log(
            `📢 Set isBroadcast to true for template message: ${msgId}`
        );
    }
    // let isBroadcast = sentMsg.isBroadcast || false;
    let pricing = null;

    if (status.pricing && status.pricing.category) {
        pricing = {
            category: status.pricing.category,
            billable: status.pricing.billable,
            pricing_model: status.pricing.pricing_model,
        };
    }

    // Step 1: Check if whatsappMessageId exists
    let template_code = null;
    if (msgId) {
        try {
            // Log template processing start
            sendDiscordMessage(
                "Template Processing Started - Outgoing",
                `🚀 Starting template lookup for outgoing message\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${sentMsg.type
                }\nStatus: ${status.status}\nExisting Record: ${sentMsg
                    ? `Type: ${sentMsg.type}, Template ID: ${sentMsg.template_id || "None"
                    }, Last Message: ${sentMsg.last_message || "None"}`
                    : "None"
                }`
            );

            // Step 2: Retry-fetch template code from collections
            if (!mongoBroadcastCollection) {
                console.error(
                    "Broadcast collection not initialized, skipping template lookup"
                );
                sendDiscordMessage(
                    "Template Lookup Error - Outgoing",
                    `🚨 Broadcast collection not initialized for message ${msgId}`
                );
            } else {
                // Use enhanced fallback helper with broadcast record fallback logic
                const result = await findTemplateCodeWithFallback(
                    msgId,
                    to, // recipient phone number for fallback query
                    !!(sentMsg && sentMsg.type === "template")
                );

                if (result && result.template_code) {
                    template_code = result.template_code;
                    console.log(
                        `Template found with retry from ${result.source}: ${template_code}`
                    );
                    sendDiscordMessage(
                        "Template Found - Outgoing",
                        `✅ Template found with retry\nMessage ID: ${msgId}\nTemplate ID: ${template_code}\nFrom: ${from}\nTo: ${to}\nSource: ${result.source}`
                    );
                } else if (!(sentMsg && sentMsg.type === "template")) {
                    // Only log info for non-template messages when no template found
                    sendDiscordMessage(
                        "No Template Found - Outgoing",
                        `ℹ️ No template found for non-template message\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${sentMsg?.type || "unknown"
                        }`
                    );
                } else {
                    console.error(
                        `🚨 Template message ${msgId} has no valid template ID found after retries`
                    );
                    sendDiscordMessage(
                        "Template ID Missing After Retries - Outgoing",
                        `🚨 Template message missing template ID after retries\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}`
                    );
                }
            }
        } catch (error) {
            console.error("Error checking template information:", error);

            // Log template lookup errors
            sendDiscordMessage(
                "Template Lookup Error - Outgoing",
                `🚨 Error checking template information for outgoing message\nMessage ID: ${msgId}\nError: ${error.message}\nStack: ${error.stack}`
            );
        }
    }

    // Update sentMsg last_message if template is found (for local object only)
    if (template_code && sentMsg) {
        // Update local sentMsg object with template
        sentMsg.last_message = `%%%template%%% ${template_code}`;

        // Also ensure template_id is set for consistency
        if (sentMsg.type === "template") {
            sentMsg.template_id = template_code;
        }

        console.log(
            `Updated local sentMsg object with template: ${template_code}`
        );

        // Log the template processing summary
        console.log(`📋 Template Processing Summary for ${msgId}:`, {
            final_template_code: template_code,
            source: template_code
                ? template_code === sentMsg?.template_id
                    ? "sentWabaMesg_fallback"
                    : "broadcast_collection"
                : "none",
            existing_record: {
                type: sentMsg?.type,
                template_id: sentMsg?.template_id,
                last_message: sentMsg?.last_message,
            },
            final_message: `%%%template%%% ${template_code}`,
        });

        // Send Discord notification about template application
        const templateSource =
            template_code === sentMsg?.template_id
                ? "sentWabaMesg collection fallback"
                : "broadcast collection";
        sendDiscordMessage(
            "Template Applied to Local Object",
            `🔄 Applied template from ${templateSource} to local sentMsg object\nMessage ID: ${msgId}\nTemplate Code: ${template_code}\nFrom: ${from}\nTo: ${to}\nUpdated Message: ${sentMsg.last_message}\nTemplate ID: ${sentMsg.template_id}\nSource: ${templateSource}`
        );

        // Validate that template processing was successful
        if (template_code) {
            console.log(
                `✅ Template processing successful for ${msgId}: ${template_code}`
            );
        } else {
            console.log(
                `⚠️ Template processing failed for ${msgId}: no template code found`
            );
        }
    }

    // Validate template messages have proper template IDs
    if (sentMsg && sentMsg.type === "template" && !template_code) {
        console.error(
            `🚨 Template message ${msgId} processed without valid template ID from any source`
        );
        sendDiscordMessage(
            "Template Validation Failed - Outgoing",
            `🚨 Template message validation failed\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${sentMsg.type
            }\nTemplate ID: ${sentMsg.template_id || "None"}\nLast Message: ${sentMsg.last_message || "None"
            }\nFallback Attempted: Yes`
        );
    }

    // Final check: ensure we have the most up-to-date template information
    // Only run this if we haven't already found a template code
    if (
        !template_code &&
        sentMsg &&
        sentMsg.type === "template" &&
        !sentMsg.last_message.startsWith("%%%template%%%")
    ) {
        console.log(
            `⚠️ Template message missing proper template string and no template code found, attempting to fix...`
        );

        try {
            // Final attempt with enhanced fallback logic
            const result = await findTemplateCodeWithFallback(
                msgId,
                to, // recipient phone number for fallback query
                true // include sentWaba fallback
            );

            if (result && result.template_code) {
                template_code = result.template_code;
                sentMsg.last_message = `%%%template%%% ${result.template_code}`;
                console.log(
                    `✅ Fixed template message with code (after retries): ${result.template_code}`
                );
            } else {
                console.error(
                    `🚨 Template message ${msgId} has no template ID found after retries in final check`
                );
                sendDiscordMessage(
                    "Template ID Missing - Final Check",
                    `🚨 Template message missing template ID in final check after retries\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${sentMsg.type}`
                );
            }
        } catch (error) {
            console.error("Error in final template fix:", error);
            sendDiscordMessage(
                "Template Fix Error - Final Check",
                `🚨 Error in final template fix\nMessage ID: ${msgId}\nError: ${error.message}`
            );
            // Don't set template_code to avoid using invalid template
        }
    }

    // Log message content for debugging
    const finalMessage = template_code
        ? `%%%template%%% ${template_code}`
        : sentMsg.type === "template"
            ? `[template message - no ID found in any source]`
            : sentMsg.type === "text" && sentMsg.payload
                ? sentMsg.payload
                : sentMsg.type === "text" && sentMsg.last_message
                    ? sentMsg.last_message
                    : sentMsg.last_message || `[${sentMsg.type || "unknown"} message]`;

    console.log(`Message processing details:`, {
        msgId: msgId,
        from: from,
        to: to,
        template_code: template_code,
        sentMsg_type: sentMsg?.type,
        sentMsg_payload: sentMsg?.payload,
        sentMsg_last_message: sentMsg?.last_message,
        conversation_origin: status.conversation?.origin?.type,
        final_message: finalMessage,
        message_source:
            sentMsg?.type === "text" && sentMsg?.payload
                ? "payload"
                : sentMsg?.type === "template" && template_code
                    ? "template_code"
                    : "last_message",
        template_processing_flow: {
            has_template_code: !!template_code,
            template_code_value: template_code,
            existing_record_has_template:
                sentMsg?.type === "template" &&
                sentMsg?.last_message?.startsWith("%%%template%%%"),
            final_message_uses_template:
                finalMessage.startsWith("%%%template%%%"),
        },
    });

    // Log final message processing result
    sendDiscordMessage(
        "Message Processing Complete - Outgoing",
        `📝 Outgoing message processing complete\nMessage ID: ${msgId}\nTemplate Applied: ${template_code ? "Yes" : "No"
        }\nTemplate Code: ${template_code || "None"
        }\nFinal Message: ${finalMessage}\nOriginal Type: ${sentMsg.type
        }\nPayload: ${sentMsg.payload || "Not available"
        }\nFrom: ${from}\nTo: ${to}`
    );

    // Debug log to show exactly what data we're using for message formatting
    console.log(`🔍 Final message formatting data for ${msgId}:`, {
        template_code: template_code,
        sentMsg_type: sentMsg?.type,
        sentMsg_last_message: sentMsg?.last_message,
        sentMsg_template_id: sentMsg?.template_id,
        template_source: template_code
            ? template_code === sentMsg?.template_id
                ? "sentWabaMesg_fallback"
                : "broadcast_collection"
            : "none",
        final_message_choice: template_code
            ? template_code === sentMsg?.template_id
                ? `template_code from sentWabaMesg_fallback (${template_code})`
                : `template_code from broadcast_collection (${template_code})`
            : sentMsg.type === "template"
                ? `template message - no ID found in any source`
                : sentMsg.type === "text" && sentMsg.payload
                    ? `sentMsg.payload (${sentMsg.payload})`
                    : sentMsg.type === "text" && sentMsg.last_message
                        ? `sentMsg.last_message (${sentMsg.last_message})`
                        : `final fallback`,
    });

    let formattedObj = {
        uid: uid,
        orgId: org_id,
        phoneNumber: from,
        nameMapping: {
            [`${to}@c.us`]: "",
            [`${from}@c.us`]: "",
        },
        chatters: {
            [`${to}@c.us`]: [
                {
                    isBroadcast: isBroadcast,
                    broadcastData: isBroadcast
                        ? {
                            pricing: pricing,
                        }
                        : null,
                    MessageId: msgId,
                    Message: template_code
                        ? `%%%template%%% ${template_code}`
                        : sentMsg.type === "template"
                            ? `[template message - no ID found in any source]`
                            : sentMsg.type === "text" && sentMsg.payload
                                ? sentMsg.payload
                                : sentMsg.type === "text" && sentMsg.last_message
                                    ? sentMsg.last_message
                                    : sentMsg.last_message ||
                                    `[${sentMsg.type || "unknown"} message]`,
                    Chatid: `${to}@c.us`,
                    File: null,
                    Ack: 1,
                    Datetime: msgTimestamp,
                    Date: new Date(msgTimestamp).toISOString().split("T")[0],
                    Direction: direction,
                    Sentbyid: `${from}@c.us`,
                    CreatedByUser: from,
                    SentByNumber: from,
                    SpecialData:
                        sentMsg.type === "text" && sentMsg.payload
                            ? { payload: sentMsg.payload }
                            : sentMsg.type === "template" && template_code
                                ? {
                                    template_id: template_code,
                                    template_message: `%%%template%%% ${template_code}`,
                                }
                                : sentMsg.last_message || {},
                    Type: sentMsg.type || "",
                },
            ],
        },
        groupParticipants: {},
    };

    return formattedObj;
}

// Check for reply payload format

async function checkForReplyFormat(eventData) {
    if (
        !eventData?.object ||
        eventData.object !== "whatsapp_business_account" ||
        !eventData.entry ||
        !Array.isArray(eventData.entry) ||
        eventData.entry.length === 0
    ) {
        return null;
    }

    const entry = eventData.entry[0];
    if (
        !entry.changes ||
        !Array.isArray(entry.changes) ||
        entry.changes.length === 0
    ) {
        return null;
    }

    const change = entry.changes[0];
    if (
        !change.value ||
        !change.value.messages ||
        !Array.isArray(change.value.messages) ||
        change.value.messages.length === 0
    ) {
        return null;
    }

    const message = change.value.messages[0];
    const metadata = change.value.metadata;
    const contact =
        change.value.contacts && change.value.contacts.length > 0
            ? change.value.contacts[0]
            : null;

    if (!message?.id || !message?.from || !metadata?.display_phone_number) {
        return false;
    }

    let specialData = {};
    let msgId = message.id;
    let msgType = message.type?.toLowerCase();

    let msg = "";
    if (msgType === "text" && message.text) {
        msg = message.text.body || "";
    } else if (message[msgType]) {
        specialData[msgType] = message[msgType];
    }

    let msgTimestamp = message.timestamp
        ? new Date(parseInt(message.timestamp) * 1000).getTime()
        : Date.now();
    let direction = "INCOMING";

    let from = metadata.display_phone_number;
    let to = message.from;
    let toName = contact?.profile?.name || "";

    if (!from || !to || !msgId || !msgTimestamp) {
        return NaN;
    }

    let org_id = null;
    let uid = null;
    try {
        const response = await axios.post(
            "https://dev.eazybe.com/v2/waba/get-embedded-signup-by-phone-number",
            { phone_number: from },
            {
                headers: {
                    "Content-Type": "application/json",
                    "private-key": "123456789",
                },
            }
        );

        if (response.data?.error) {
            return null;
        }
        uid = response.data?.data?.workapace_id;
        org_id = response.data?.data?.org_id;
    } catch (err) {
        console.log(
            "---------------ERROR START while fetching org id from API-------------"
        );
        console.log(err);
        console.log(
            "---------------ERROR END while fetching org id from API-------------"
        );
    }

    if (!org_id) {
        console.log("⚠️ No org_id found, using default for testing");
        org_id = "12345"; // Use default org_id for testing
    }

    if (!uid) {
        console.log("⚠️ No workspace_id found, using default for testing");
        uid = "67890"; // Use default workspace_id for testing
    }

    // Incoming messages are user replies, not template messages
    // No template processing needed for incoming messages

    // Log final message processing result for incoming message
    sendDiscordMessage(
        "Message Processing Complete - Incoming",
        `📝 Incoming message processing complete\nMessage ID: ${msgId}\nMessage: ${msg}\nMessage Type: ${msgType}\nFrom: ${to}\nTo: ${from}`
    );

    let formattedObj = {
        orgId: org_id,
        uid: uid,
        phoneNumber: from,
        nameMapping: {
            [`${to}@c.us`]: toName,
            [`${from}@c.us`]: "",
        },
        chatters: {
            [`${to}@c.us`]: [
                {
                    isBroadcast: false,
                    broadcastData: null,
                    MessageId: msgId,
                    Message: msg,
                    Chatid: `${to}@c.us`,
                    Type: msgType,
                    File: null,
                    Ack: 1,
                    Datetime: msgTimestamp,
                    Date: new Date(msgTimestamp).toISOString().split("T")[0],
                    Direction: direction,
                    Sentbyid: `${from}@c.us`,
                    CreatedByUser: from,
                    SentByNumber: from,
                    SpecialData:
                        msgType === "text"
                            ? { payload: msg, ...specialData }
                            : specialData,
                },
            ],
        },
        groupParticipants: {},
    };

    return formattedObj;
}

async function detectingAndModifyingDataFormat(eventData) {
    // console.log("🔍 detectingAndModifyingDataFormat called with:", JSON.stringify(eventData, null, 2));

    // Log data structure analysis (essential for debugging)
    if (!eventData?.orgId || !eventData?.phoneNumber || !eventData?.chatters) {
        console.log("📊 Data structure analysis: Missing required fields");
        console.log(`  orgId: ${eventData?.orgId ? '✅' : '❌'}`);
        console.log(`  phoneNumber: ${eventData?.phoneNumber ? '✅' : '❌'}`);
        console.log(`  chatters: ${eventData?.chatters ? '✅' : '❌'}`);
    }

    if (
        // eventData?.mode && eventData?.mode === "EXT" &&
        eventData?.orgId &&
        eventData?.phoneNumber &&
        eventData?.nameMapping &&
        eventData?.chatters
        // eventData?.groupParticipants
    ) {
        console.log("✅ Data format is already correct, returning as-is");
        return eventData;
    }

    console.log("🔄 Data format needs processing, checking for send/reply formats...");


    console.log(
        "------------****************> extractedData 1start <---------------------------"
    );
    console.log(JSON.stringify(eventData));
    console.log(
        "------------------------> extractedData 1end <****************---------------"
    );

    let sendFormatResult = await checkForSendFormat(eventData);
    // Discord log for sendFormatResult

    console.log(JSON.stringify(sendFormatResult));
    console.log("--> sendFormatResult");
    if (sendFormatResult) {
        await sendDiscordMessage(
            "detectingAndModifyingDataFormat",
            `sendFormatResult:\n${JSON.stringify(sendFormatResult)}`
        );
        return sendFormatResult;
    }

    let replyFormatResult = await checkForReplyFormat(eventData);
    // Discord log for replyFormatResult
    console.log(JSON.stringify(replyFormatResult));
    console.log("--> replyFormatResult");
    if (replyFormatResult) {
        return replyFormatResult;
    }


    console.log("---------------------2 START----------------------");
    console.log(JSON.stringify(eventData));
    console.log("---------------------2 END----------------------");

    return null;
}

async function updatePubSubMessageStatus(
    messageId,
    status,
    startTime,
    errorMessage
) {
    await mongoPubSubMessagesCollection.updateOne(
        { messageId: messageId },
        {
            $set: {
                status: status,
                updatedAt: new Date(),
                ...(status === "completed"
                    ? { timeTaken: Date.now() - startTime }
                    : {}),
                ...(errorMessage ? { errorMessage: errorMessage } : {}),
            },
            $inc: { calls: 1 },
        }
    );
}

// ENHANCED: Comprehensive backup validation
const validateBackupIntegrity = async (originalEventData, processedDateAccChats) => {
    try {
        console.log("🔍 Starting comprehensive backup validation...");

        let originalMessageCount = 0;
        let processedMessageCount = 0;
        let missingMessages = [];

        // Count original messages
        for (let chatter in originalEventData.chatters) {
            originalMessageCount += originalEventData.chatters[chatter].length;
        }

        // Count processed messages
        for (let date in processedDateAccChats) {
            for (let chatter in processedDateAccChats[date]) {
                processedMessageCount += processedDateAccChats[date][chatter].length;
            }
        }

        // Find missing messages
        for (let chatter in originalEventData.chatters) {
            const originalMessages = originalEventData.chatters[chatter];
            let foundInProcessed = false;

            // Check if this chatter exists in any date
            for (let date in processedDateAccChats) {
                if (processedDateAccChats[date][chatter]) {
                    foundInProcessed = true;
                    break;
                }
            }

            if (!foundInProcessed) {
                missingMessages.push({
                    chatter: chatter,
                    reason: "Chatter completely missing from processed data",
                    messages: originalMessages
                });
            }
        }

        // Log validation results
        console.log(`📊 Backup Validation Results:`);
        console.log(`  Original messages: ${originalMessageCount}`);
        console.log(`  Processed messages: ${processedMessageCount}`);
        console.log(`  Missing messages: ${originalMessageCount - processedMessageCount}`);
        console.log(`  Validation: ${originalMessageCount === processedMessageCount ? '✅ PASS' : '❌ FAIL'}`);

        if (missingMessages.length > 0) {
            console.log(`🚨 Missing chatters: ${missingMessages.length}`);
            missingMessages.forEach(missing => {
                console.log(`   - ${missing.chatter}: ${missing.reason} (${missing.messages.length} messages)`);
            });

            // Send critical alert
            await sendDiscordMessage(
                "CRITICAL: Backup Validation Failed",
                `🚨 BACKUP VALIDATION FAILED\nOriginal: ${originalMessageCount} messages\nProcessed: ${processedMessageCount} messages\nMissing: ${originalMessageCount - processedMessageCount} messages\nMissing Chatters: ${missingMessages.length}`
            );

            return false;
        }

        return originalMessageCount === processedMessageCount;

    } catch (error) {
        console.error("Error during backup validation:", error);
        await sendDiscordMessage(
            "Backup Validation Error",
            `❌ Error during backup validation\nError: ${error.message}`
        );
        return false;
    }
};

// EMERGENCY: Backup for missing messages
const emergencyBackupMissingMessages = async (eventData, processedDateAccChats) => {
    try {
        console.log("🚨 Starting emergency backup for missing messages...");

        const emergencyFilePath = `${eventData.orgId}/emergency_backup_${Date.now()}.json`;
        const emergencyFile = bucket.file(emergencyFilePath);

        const emergencyData = {
            orgId: eventData.orgId,
            phoneNumber: eventData.phoneNumber,
            timestamp: new Date().toISOString(),
            originalEventData: eventData,
            processedData: processedDateAccChats,
            validation: "EMERGENCY_BACKUP"
        };

        await saveFileWithLock(emergencyFile, JSON.stringify(emergencyData, null, 2));
        console.log(`✅ Emergency backup saved: ${emergencyFilePath}`);

        await sendDiscordMessage(
            "Emergency Backup Created",
            `🚨 Emergency backup created due to validation failure\nPath: ${emergencyFilePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}`
        );

    } catch (error) {
        console.error("Error creating emergency backup:", error);
        await sendDiscordMessage(
            "Emergency Backup Failed",
            `💥 Emergency backup failed\nError: ${error.message}`
        );
    }
};

const mainEngine = async (req, res) => {

    // ENHANCED BACKUP VALIDATION with comprehensive checking
    try {
        const validationResult = await validateBackupIntegrity(eventData, dateAccChats);

        if (!validationResult) {
            // If validation failed, try emergency backup for missing messages
            await emergencyBackupMissingMessages(eventData, dateAccChats);
        } else {
            console.log(`✅ Backup validation passed! All ${totalMessages} messages successfully processed!`);

            // Send success notification
            await sendDiscordMessage(
                "Backup Success",
                `✅ Backup completed successfully\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nMessages: ${totalMessages}\nDates: ${Object.keys(dateAccChats).length}`
            );
        }
    } catch (validationError) {
        console.error("Error during backup validation:", validationError);
        await sendDiscordMessage(
            "Backup Validation Error",
            `❌ Error during backup validation\nError: ${validationError.message}`
        );
    }
    try {
        console.log("🚀 mainEngine started");
        const startTime = Date.now();
        const extractedData = extractEventData(req);
        // console.log("📥 Extracted data:", JSON.stringify(extractedData, null, 2));

        // Log raw request body for debugging (only if needed)
        // console.log("🔍 RAW REQUEST BODY:", JSON.stringify(req.body, null, 2));

        if (!extractedData.workspace_id) {
            // sendDiscordMessage("Extracted Data", JSON.stringify(extractedData));
        }
        let eventData = await detectingAndModifyingDataFormat(extractedData);
        // console.log("🔄 Event data after format detection:", JSON.stringify(eventData, null, 2));

        // Log final event data structure (essential for debugging)
        if (eventData && eventData.chatters) {
            console.log("📊 Event data analysis: Total chatters:", Object.keys(eventData.chatters).length);
            for (let chatter in eventData.chatters) {
                console.log(`  📱 ${chatter}: ${eventData.chatters[chatter].length} messages`);
            }
        } else {
            console.error("❌ CRITICAL: eventData.chatters is missing or invalid!");
        }

        if (!eventData.workspace_id) {
            // sendDiscordMessage("Event Data", JSON.stringify(eventData));
        }

        console.log("🔍 Checking for existing PubSub message...");
        const messageId = req?.body?.message?.messageId || "direct_request_" + Date.now();
        console.log("📋 Message ID:", messageId);
        let pubsubMessage = await mongoPubSubMessagesCollection.findOne({
            messageId: messageId,
        });
        console.log("📋 PubSub message check result:", pubsubMessage);
        if (pubsubMessage) {
            if (pubsubMessage.status === "processing") {
                updatePubSubMessageStatus(
                    req?.body?.message?.messageId,
                    "processing",
                    startTime
                );
                return "processing";
            }
            if (pubsubMessage.status === "completed") {
                updatePubSubMessageStatus(
                    req?.body?.message?.messageId,
                    "completed",
                    startTime
                );
                return true;
            }
            if (pubsubMessage.status === "error") {
                updatePubSubMessageStatus(
                    req?.body?.message?.messageId,
                    "error",
                    startTime
                );
                return false;
            }
        }

        console.log("📝 Inserting new PubSub message record...");
        await mongoPubSubMessagesCollection.insertOne({
            messageId: messageId,
            phoneNumber: eventData?.phoneNumber || null,
            publishTime: req?.body?.message?.publishTime,
            status: "processing",
            calls: 0,
            timeTaken: null,
            createdAt: new Date(),
        });
        console.log("✅ PubSub message record inserted successfully");

        if (!eventData || typeof eventData !== "object") {
            console.log("❌ Event data is not a valid object:", eventData);
            //   sendDiscordMessage(
            //     'Event Data Type Error',
            //     `Extracted event data is not a valid object: ${JSON.stringify(
            //       eventData,
            //     )}`,
            //   );
            return false;
        }

        if (!eventData?.orgId) {
            console.log("❌ Missing orgId");
            return false;
        }
        if (!eventData?.phoneNumber) {
            console.log("❌ Missing phoneNumber");
            return false;
        }
        if (!eventData?.nameMapping) {
            console.log("❌ Missing nameMapping");
            return false;
        }
        if (!eventData?.chatters) {
            console.log("❌ Missing chatters");
            return false;
        }
        if (!eventData?.groupParticipants) {
            console.log("❌ Missing groupParticipants");
            return false;
        }

        console.log("✅ All required fields present, proceeding with processing...");

        // Validate backup prerequisites
        console.log("🔍 Validating backup prerequisites...");
        if (
            !eventData.orgId ||
            !eventData.phoneNumber ||
            !eventData.chatters ||
            Object.keys(eventData.chatters).length === 0
        ) {
            const errorMsg = `Backup prerequisites not met:\nOrg ID: ${eventData.orgId
                }\nPhone: ${eventData.phoneNumber}\nChatters: ${Object.keys(eventData.chatters).length
                }`;
            console.error(errorMsg);
            await sendDiscordMessage(
                "Backup Prerequisites Failed",
                `❌ ${errorMsg}`
            );
            return false;
        }

        // Check if workspace_id exists and skip backup if phone is already connected to WABA
        if (eventData.workspace_id) {
            try {
                console.log(
                    `Workspace ID detected: ${eventData.workspace_id}, checking WABA connection status for phone: ${eventData.phoneNumber}`
                );

                const wabaResponse = await axios.get(
                    `https://dev.eazybe.com/v2/waba/check-phone-number?phoneNumber=${eventData.phoneNumber}`,
                    {
                        timeout: 10000, // 10 second timeout
                        headers: {
                            "Content-Type": "application/json",
                        },
                    }
                );

                if (
                    wabaResponse.data?.status &&
                    wabaResponse.data?.data?.status &&
                    wabaResponse.data?.data?.data?.isWabaConnected
                ) {
                    console.log(
                        `Phone ${eventData.phoneNumber} is already connected to WABA, skipping backup process`
                    );

                    // Send Discord notification about skipping backup
                    // sendDiscordMessage(
                    //     "WABA Backup Skipped",
                    //     `⏭️ Backup process skipped for workspace ${eventData.workspace_id}\nPhone: ${eventData.phoneNumber}\nReason: Phone already connected to WABA\nStatus: ${wabaResponse.data?.data?.message}`
                    // );

                    // Update PubSub message status and return early
                    if (req?.body?.message?.messageId) {
                        await updatePubSubMessageStatus(
                            req.body.message.messageId,
                            "completed",
                            startTime,
                            "Backup skipped - Phone already connected to WABA"
                        );
                    }

                    return true; // Skip backup process
                } else {
                    console.log(
                        `Phone ${eventData.phoneNumber} is not connected to WABA, proceeding with backup process`
                    );
                }
            } catch (error) {
                console.error(
                    `Error checking WABA connection status for phone ${eventData.phoneNumber}:`,
                    error.message
                );

                // If API call fails, log error but continue with backup process
                sendDiscordMessage(
                    "WABA Check Error",
                    `⚠️ Error checking WABA connection status\nPhone: ${eventData.phoneNumber}\nWorkspace: ${eventData.workspace_id}\nError: ${error.message}\nProceeding with backup process as fallback`
                );
            }
        }

        // If no workspace_id, proceed with normal backup process
        if (!eventData.workspace_id) {
            sendDiscordMessage(
                "WABA Backup Proceeding",
                `📋 Proceeding with normal backup process (no workspace_id)\nPhone: ${eventData.phoneNumber}\nOrg ID: ${eventData.orgId}`
            );
        }

        console.log("📊 Creating dateAccChats object...");
        let dateAccChats = {};
        let toPhoneNumberArray = Object.keys(eventData.chatters).map(
            (chatter) => chatter.split("@")[0]
        );
        console.log("📱 Total chatters =>", toPhoneNumberArray.length);

        // Count total messages being processed
        let totalMessages = 0;
        let uniqueMessageIds = new Set();
        let duplicateMessageIds = new Set();

        for (let chatter in eventData.chatters) {
            const chatterMessages = eventData.chatters[chatter];
            totalMessages += chatterMessages.length;

            // Check for duplicate MessageIds in the incoming data
            chatterMessages.forEach(msg => {
                if (uniqueMessageIds.has(msg.MessageId)) {
                    duplicateMessageIds.add(msg.MessageId);
                    console.warn(`🚨 DUPLICATE MessageId detected in incoming data: ${msg.MessageId} (${msg.Message})`);
                } else {
                    uniqueMessageIds.add(msg.MessageId);
                }
            });
        }

        console.log(`📨 Total messages to process: ${totalMessages}`);
        console.log(`📋 Unique MessageIds: ${uniqueMessageIds.size}`);
        console.log(`⚠️ Duplicate MessageIds in incoming data: ${duplicateMessageIds.size}`);

        if (duplicateMessageIds.size > 0) {
            console.log(`🚨 Duplicate MessageIds found:`, Array.from(duplicateMessageIds));
            await sendDiscordMessage(
                "Duplicate MessageIds Detected",
                `🚨 Found ${duplicateMessageIds.size} duplicate MessageIds in incoming webhook data\nDuplicates: ${Array.from(duplicateMessageIds).join(', ')}\nTotal Messages: ${totalMessages}\nUnique Messages: ${uniqueMessageIds.size}`
            );
        }

        // Log message inventory (essential for debugging)
        console.log(`📨 Message inventory: ${totalMessages} total messages`);
        for (let chatter in eventData.chatters) {
            console.log(`  📱 ${chatter}: ${eventData.chatters[chatter].length} messages`);

            // CRITICAL: Analyze message sequence for gaps
            const messages = eventData.chatters[chatter];
            if (messages.length > 0) {
                console.log(`🔍 Analyzing message sequence for ${chatter}:`);

                // Sort messages by timestamp
                const sortedMessages = messages.sort((a, b) => a.Datetime - b.Datetime);

                // Check for gaps in sequence
                let sequenceGaps = [];
                for (let i = 1; i < sortedMessages.length; i++) {
                    const prevMsg = sortedMessages[i - 1];
                    const currMsg = sortedMessages[i];
                    const timeDiff = currMsg.Datetime - prevMsg.Datetime;

                    // If there's a significant time gap (> 2 seconds), it might indicate missing messages
                    if (timeDiff > 2000) {
                        sequenceGaps.push({
                            gap: timeDiff,
                            between: `${prevMsg.Message} → ${currMsg.Message}`,
                            timestamps: `${new Date(prevMsg.Datetime).toISOString()} → ${new Date(currMsg.Datetime).toISOString()}`
                        });
                    }
                }

                if (sequenceGaps.length > 0) {
                    console.log(`⚠️ SEQUENCE GAPS DETECTED in ${chatter}:`);
                    sequenceGaps.forEach((gap, index) => {
                        console.log(`  ${index + 1}. Gap: ${gap.gap}ms between ${gap.between}`);
                        console.log(`     ${gap.timestamps}`);
                    });

                    // Send Discord alert for sequence gaps
                    await sendDiscordMessage(
                        "Message Sequence Gaps Detected",
                        `⚠️ SEQUENCE GAPS DETECTED\nChatter: ${chatter}\nGaps: ${sequenceGaps.length}\nDetails:\n${sequenceGaps.map(gap => `- ${gap.gap}ms between ${gap.between}`).join('\n')}`
                    );
                } else {
                    console.log(`✅ No sequence gaps detected in ${chatter}`);
                }

                // Log message details for analysis
                console.log(`📋 Message details for ${chatter}:`);
                sortedMessages.forEach((msg, index) => {
                    console.log(`  ${index + 1}. ${msg.Message} (${msg.MessageId}) - ${new Date(msg.Datetime).toISOString()}`);
                });

                // CRITICAL: Use enhanced sequence tracking
                const sequenceResult = await trackMessageSequence(chatter, messages);

                if (sequenceResult) {
                    console.log(`🔢 Sequence tracking completed for ${chatter}:`);
                    console.log(`  Sequence: ${sequenceResult.sequenceName}`);
                    console.log(`  Range: ${sequenceResult.minNumber} to ${sequenceResult.maxNumber}`);
                    console.log(`  Received: ${sequenceResult.received} messages`);
                    console.log(`  Missing in batch: ${sequenceResult.missingInBatch}`);
                    console.log(`  Total missing: ${sequenceResult.totalMissing}`);
                    console.log(`  Total received: ${sequenceResult.totalReceived}`);
                }
            }
        }

        // CRITICAL FIX: Clean up duplicate MessageIds from incoming data
        if (duplicateMessageIds.size > 0) {
            console.log(`🧹 Cleaning up ${duplicateMessageIds.size} duplicate MessageIds from incoming data...`);

            for (let chatter in eventData.chatters) {
                const originalCount = eventData.chatters[chatter].length;
                const seenMessageIds = new Set();

                // Keep only the first occurrence of each MessageId
                eventData.chatters[chatter] = eventData.chatters[chatter].filter(msg => {
                    if (seenMessageIds.has(msg.MessageId)) {
                        console.log(`🗑️ Removing duplicate MessageId: ${msg.MessageId} (${msg.Message})`);
                        return false;
                    }
                    seenMessageIds.add(msg.MessageId);
                    return true;
                });

                const cleanedCount = eventData.chatters[chatter].length;
                if (originalCount !== cleanedCount) {
                    console.log(`✅ Cleaned ${chatter}: ${originalCount} → ${cleanedCount} messages (removed ${originalCount - cleanedCount} duplicates)`);
                }
            }

            // Recalculate totals after cleanup
            totalMessages = 0;
            for (let chatter in eventData.chatters) {
                totalMessages += eventData.chatters[chatter].length;
            }
            console.log(`📊 After cleanup: ${totalMessages} unique messages to process`);
        }


        let startTimeLastMsgOne = Date.now();

        // CRITICAL FIX: Skip last message time lookup to prevent message filtering
        // The previous logic was causing messages to be filtered out based on timestamps
        // We want to backup ALL messages regardless of when they were sent

        // Keep the map empty to ensure no filtering occurs
        let lastMessageOfChattersMap = {};

        // FIXED: Enhanced message processing without aggressive filtering
        console.log(`🔄 Processing messages into dateAccChats...`);
        for (let chatter in eventData.chatters) {
            // Sort messages by timestamp to maintain sequence
            const sortedMessages = eventData.chatters[chatter].sort((a, b) => a.Datetime - b.Datetime);

            let processedCount = 0;
            let skippedCount = 0;

            for (let chat of sortedMessages) {
                let chatDate = chat.Date;

                // Validate message has required fields but don't skip if some are missing
                if (!chat.MessageId) {
                    // Generate a MessageId if missing to prevent data loss
                    chat.MessageId = `missing_id_${chatter}_${chat.Datetime}_${Math.random().toString(36).substr(2, 9)}`;
                    console.warn(`⚠️ Generated MessageId for message: ${chat.MessageId}`);
                }

                if (!chat.Datetime) {
                    // Use current timestamp if missing
                    chat.Datetime = Date.now();
                    console.warn(`⚠️ Generated timestamp for message: ${chat.MessageId}`);
                }

                if (!dateAccChats.hasOwnProperty(chatDate)) {
                    dateAccChats[chatDate] = { [chatter]: [chat] };
                } else {
                    if (!dateAccChats[chatDate][chatter]) {
                        dateAccChats[chatDate][chatter] = [chat];
                    } else {
                        dateAccChats[chatDate][chatter].push(chat);
                    }
                }
                processedCount++;
            }

            console.log(`📊 ${chatter}: ${processedCount} messages processed`);
        }

        // Count messages in dateAccChats after processing
        let processedMessages = 0;
        for (let date in dateAccChats) {
            for (let chatter in dateAccChats[date]) {
                processedMessages += dateAccChats[date][chatter].length;
            }
        }
        if (processedMessages !== totalMessages) {
            console.log(`⚠️ Message count mismatch: ${totalMessages} received, ${processedMessages} processed`);
        }

        // Process data for BigQuery after dateAccChats is created
        try {
            console.log("🚀 Starting BigQuery processing...");
            const bigQueryResult = await bigQueryProcessor(dateAccChats, eventData.orgId, eventData.uid ?? eventData.workspace_id ?? null);
            // console.log(`✅ BigQuery processing completed: ${bigQueryResult}`);

            // Log successful BigQuery processing
            // sendDiscordMessage(
            //     "BigQuery Processing Success",
            //     `✅ Successfully processed data for BigQuery\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nResult: ${bigQueryResult}`
            // );
        } catch (bigQueryError) {
            console.error("Error processing data for BigQuery:", bigQueryError);
            await sendDiscordMessage(
                "BigQuery Processing Error",
                `❌ Failed to process data for BigQuery\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${bigQueryError.message}\nStack: ${bigQueryError.stack}`
            );
            // Don't throw error - continue with file processing even if BigQuery fails
        }

        let startTimeLastMsg = Date.now();
        try {
            await createLastMessages(
                eventData?.orgId,
                eventData?.phoneNumber,
                eventData.chatters,
                eventData.nameMapping,
                eventData?.profileImages || {}
            );
            let timeTakenLastMsg = Date.now() - startTimeLastMsg;

            // Log successful backup creation
            // await sendDiscordMessage(
            //     "Backup Creation Success",
            //     `✅ Successfully created/updated last messages\nOrg ID: ${
            //         eventData?.orgId
            //     }\nPhone: ${eventData?.phoneNumber}\nChatters: ${
            //         Object.keys(eventData.chatters).length
            //     }\nTime: ${timeTakenLastMsg}ms`
            // );
        } catch (error) {
            console.error("Error creating last messages:", error);
            await sendDiscordMessage(
                "Backup Creation Error",
                `❌ Failed to create/update last messages\nOrg ID: ${eventData?.orgId}\nPhone: ${eventData?.phoneNumber}\nError: ${error.message}\nStack: ${error.stack}`
            );
        }


        // Log file processing start
        const totalDates = Object.keys(dateAccChats).length;
        let processedDates = 0;

        // await sendDiscordMessage(
        //     "File Processing Started",
        //     `📁 Starting file processing\nTotal Dates: ${totalDates}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}`
        // );

        for (let chatKey in dateAccChats) {
            processedDates++;
            const filePath = `${eventData.orgId}/${chatKey.split("-")[0]}/${chatKey.split("-")[1]
                }/${chatKey.split("-")[2]}/${eventData.phoneNumber}.json`;

            // Count messages for this date
            let messagesForDate = 0;
            for (let chatter in dateAccChats[chatKey]) {
                messagesForDate += dateAccChats[chatKey][chatter].length;
            }
            console.log(`📅 Processing date ${chatKey}: ${messagesForDate} messages`);

            const file = await bucket.file(filePath);
            let [fileExists] = await file.exists();

            // Log progress
            if (processedDates % 5 === 0 || processedDates === totalDates) {
                // await sendDiscordMessage(
                //     "File Processing Progress",
                //     `📊 Processing progress: ${processedDates}/${totalDates} dates\nCurrent: ${chatKey}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}`
                // );
            }

            if (!fileExists) {
                let fileData = {
                    orgId: eventData.orgId,
                    phoneNumber: eventData.phoneNumber,
                    nameMapping: eventData.nameMapping,
                    chatters: dateAccChats[chatKey],
                    groupParticipants: eventData?.groupParticipants,
                };

                try {
                    await saveFileWithLock(file, JSON.stringify(fileData, null, 2));
                    console.log(`✅ New file created successfully: ${filePath}`);
                } catch (uploadError) {
                    console.error(`❌ Failed to create new file ${filePath}:`, uploadError);

                    // Log upload error
                    await sendDiscordMessage(
                        "GCS Upload Error",
                        `❌ GCS upload failed\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${uploadError.message}`
                    );

                    throw uploadError; // Re-throw to be caught by outer error handler
                }
                continue;
            }

            try {
                console.log(`🔧 Processing existing file: ${filePath} (${messagesForDate} messages)`);

                // Use the new enhanced file processing function
                const processResult = await processFileWithLock(file, eventData, dateAccChats, chatKey);

                if (!processResult) {
                    console.error(`❌ File processing failed: ${filePath}`);
                    throw new Error(`File processing failed for ${filePath}`);
                }

            } catch (error) {
                console.error(`❌ Error processing existing file ${filePath}:`, error);

                // Log error
                await sendDiscordMessage(
                    "GCS File Processing Error",
                    `❌ Error processing file\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${error.message}\nStack: ${error.stack}`
                );

                throw error; // Re-throw to be caught by outer error handler
            }
        }

        if (eventData && eventData.workspace_id) {
            try {
                const currentDate = new Date().toISOString();

                const syncResponse = await axios.post(
                    "https://dev.eazybe.com/v2/users/last-chat-sync-time",
                    {
                        workspace_id: Number(eventData.workspace_id),
                        last_chat_sync_time: currentDate,
                    },
                    {
                        headers: {
                            "private-key": "123456789",
                            "Content-Type": "application/json",
                        },
                        timeout: 10000,
                    }
                );

                console.log(
                    `Updated last chat sync time for workspace_id: ${eventData.workspace_id}`
                );

                // Log successful sync update
                // await sendDiscordMessage(
                //     "Sync Time Updated",
                //     `✅ Updated last chat sync time\nWorkspace ID: ${eventData.workspace_id}\nSync Time: ${currentDate}\nResponse: ${syncResponse.status}`
                // );
            } catch (apiError) {
                console.error(
                    "Error updating last chat sync time:",
                    apiError.message
                );

                // Log sync update error
                // await sendDiscordMessage(
                //     "Sync Time Update Failed",
                //     `❌ Failed to update last chat sync time\nWorkspace ID: ${eventData.workspace_id}\nError: ${apiError.message}`
                // );
            }
        }

        // ENHANCED BACKUP VALIDATION with detailed message tracking
        try {
            const totalBackedUpMessages = Object.values(dateAccChats).reduce(
                (total, chatterData) => {
                    return (
                        total +
                        Object.values(chatterData).reduce(
                            (chatterTotal, messages) => {
                                return chatterTotal + messages.length;
                            },
                            0
                        )
                    );
                },
                0
            );

            // Validation logging
            console.log(`🔍 Backup validation: ${totalMessages} received, ${totalBackedUpMessages} processed`);

            if (totalBackedUpMessages !== totalMessages) {
                console.log(`📊 Breakdown by date:`);
                for (let date in dateAccChats) {
                    const dateMessages = Object.values(dateAccChats[date]).reduce((total, messages) => total + messages.length, 0);
                    console.log(`  📅 ${date}: ${dateMessages} messages`);
                }
            }

            // Validate message integrity
            let validationErrors = [];
            for (let date in dateAccChats) {
                for (let chatter in dateAccChats[date]) {
                    const messages = dateAccChats[date][chatter];
                    messages.forEach((msg, index) => {
                        if (!msg.MessageId || !msg.Message || !msg.Datetime) {
                            validationErrors.push({
                                date,
                                chatter,
                                index,
                                message: msg,
                                error: 'Missing required fields'
                            });
                        }
                    });
                }
            }

            if (validationErrors.length > 0) {
                console.error(`❌ Found ${validationErrors.length} validation errors:`);
                validationErrors.forEach(error => {
                    console.error(`  - ${error.date}/${error.chatter}[${error.index}]: ${error.error}`, error.message);
                });
            }

            if (totalBackedUpMessages !== totalMessages) {
                const lostMessages = totalMessages - totalBackedUpMessages;
                console.warn(`⚠️ MESSAGE LOSS DETECTED: ${lostMessages} messages lost`);

                // Send critical alert
                await sendDiscordMessage(
                    "CRITICAL: Message Loss Detected",
                    `🚨 MESSAGE LOSS DETECTED\nOriginal: ${totalMessages}\nProcessed: ${totalBackedUpMessages}\nLost: ${lostMessages}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}`
                );
            } else {
                console.log(`✅ All ${totalMessages} messages successfully processed!`);
            }

        } catch (validationError) {
            console.error("Error during backup validation:", validationError);
            await sendDiscordMessage(
                "Backup Validation Error",
                `❌ Error during backup validation\nError: ${validationError.message}\nStack: ${validationError.stack}`
            );
        }

        console.log(`------ REQUEST COMPLETED : ${count++} ------`);

        return true;
    } catch (error) {
        console.error("Error processing webhook (inner):", error);

        // Send comprehensive error notification
        await sendDiscordMessage(
            "Backup Process Failed",
            `💥 Backup process failed\nOrg ID: ${eventData?.orgId || "Unknown"
            }\nPhone: ${eventData?.phoneNumber || "Unknown"}\nError: ${error.message
            }\nStack: ${error.stack}\nTime: ${Date.now() - startTime}ms`
        );

        return false;
    }
};

let count = 0;
const webhookProcessor = async (req, res) => {
    // const webhookProcessor = async (req, res) => {
    try {
        const webhookId = req?.body?.message?.messageId || `direct_${Date.now()}`;
        const requestTimestamp = new Date().toISOString();

        console.log("🎯 webhookProcessor called");
        console.log(`📋 Webhook ID: ${webhookId}`);
        console.log(`⏰ Request timestamp: ${requestTimestamp}`);
        console.log(`📊 Request body size: ${JSON.stringify(req.body).length} bytes`);

        // Log webhook request details for monitoring
        await sendDiscordMessage(
            "Webhook Request Received",
            `📥 New webhook request received\nWebhook ID: ${webhookId}\nTimestamp: ${requestTimestamp}\nBody size: ${JSON.stringify(req.body).length} bytes\nSource: ${req.ip || 'unknown'}`
        );

        const startTime = Date.now();
        mainEngine(req, res)
            .then((status) => {
                if (status === "processing") {
                    return;
                }
                if (status) {
                    updatePubSubMessageStatus(
                        req?.body?.message?.messageId,
                        "completed",
                        startTime
                    );
                    //   sendDiscordMessage(
                    //     'Time taken',
                    //     `Time taken: ${(Date.now() - startTime) / 1000}s`,
                    //   );
                } else {
                    throw new Error(
                        "Unacknowledged event with messageId: " +
                        req?.body?.message?.messageId
                    );
                }
            })
            .catch((error) => {
                // sendDiscordMessage('Error', `Error in webhook processor: ${error}`);
                updatePubSubMessageStatus(
                    req?.body?.message?.messageId,
                    "error"
                );
            });
        // sendDiscordMessage('Total calls', ++count);
        return res.status(200).json({
            status: "success",
            message: "Event Captured",
            messageId: req?.body?.message?.messageId || "direct_request",
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        // sendDiscordMessage('Error', `Error processing webhook (outer): ${error}`);
        count++;
        return res.status(500).send("Error Occurred");
    }
};




// app.post("/", async (req, res) => {
//     return (await webhookProcessor(req, res))
// });

// app.listen(3003, () => {
//     console.log("Server is listening on PORT =>", 3003);
// });

// Export for external use
exports.webhookProcessor = webhookProcessor;