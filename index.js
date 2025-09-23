const express = require("express");
const { Storage } = require("@google-cloud/storage");
const { MongoClient } = require("mongodb"); // Using JS Date, Timestamp not strictly needed here
const axios = require("axios");
const { bigQueryProcessor } = require("./bigQueryProcessor");

// Set Google Cloud credentials environment variable
process.env.GOOGLE_APPLICATION_CREDENTIALS = "./gcp-key.json";

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
        console.error("Error sending Discord message:", error);
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
    projectId: "waba-454907",
    keyFilename: "gcp-key.json",
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
            // Try broadcast collection first
            if (mongoBroadcastCollection) {
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
                `Retry attempt ${
                    attempt + 1
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
                const filePath = `${orgId}/${chat.Date.split("-")[0]}/${
                    chat.Date.split("-")[1]
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
                // Try to find template code from broadcast collection
                const broadcastRecord = await mongoBroadcastCollection.findOne({
                    whatsapp_message_id: msgId,
                });

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
                        const result = await findTemplateCodeWithRetry(msgId, {
                            includeSentWabaFallback: false,
                            attempts: 4,
                            initialDelayMs: 1500,
                            factor: 3,
                        });

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
        org_id = "12345"; // Use default org_id for testing
    }
    
    if (!uid) {
        console.log("⚠️ No uid found, using default for testing");
        uid = "67890"; // Use default workspace_id for testing
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
                `🚀 Starting template lookup for outgoing message\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${
                    sentMsg.type
                }\nStatus: ${status.status}\nExisting Record: ${
                    sentMsg
                        ? `Type: ${sentMsg.type}, Template ID: ${
                              sentMsg.template_id || "None"
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
                // Use retry helper with fallback to sentWabaMesg for template messages
                const result = await findTemplateCodeWithRetry(msgId, {
                    includeSentWabaFallback: !!(
                        sentMsg && sentMsg.type === "template"
                    ),
                    attempts: 4,
                    initialDelayMs: 1500,
                    factor: 3,
                });

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
                        `ℹ️ No template found for non-template message\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${
                            sentMsg?.type || "unknown"
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
            `🚨 Template message validation failed\nMessage ID: ${msgId}\nFrom: ${from}\nTo: ${to}\nMessage Type: ${
                sentMsg.type
            }\nTemplate ID: ${sentMsg.template_id || "None"}\nLast Message: ${
                sentMsg.last_message || "None"
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
            // Final attempt with backoff
            const result = await findTemplateCodeWithRetry(msgId, {
                includeSentWabaFallback: true,
                attempts: 4,
                initialDelayMs: 1500,
                factor: 3,
            });

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
        `📝 Outgoing message processing complete\nMessage ID: ${msgId}\nTemplate Applied: ${
            template_code ? "Yes" : "No"
        }\nTemplate Code: ${
            template_code || "None"
        }\nFinal Message: ${finalMessage}\nOriginal Type: ${
            sentMsg.type
        }\nPayload: ${
            sentMsg.payload || "Not available"
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

    // Step 1: Check if whatsappMessageId exists
    let template_code = null;
    if (msgId) {
        try {
            // Log template processing start for incoming message
            sendDiscordMessage(
                "Template Processing Started - Incoming",
                `🚀 Starting template lookup for incoming message\nMessage ID: ${msgId}\nFrom: ${to}\nTo: ${from}\nMessage Type: ${msgType}\nMessage Content: ${msg}`
            );

            // Step 2: Find matching record in broadcast collection
            if (!mongoBroadcastCollection) {
                console.error(
                    "Broadcast collection not initialized, skipping template lookup"
                );
                sendDiscordMessage(
                    "Template Lookup Error - Incoming",
                    `🚨 Broadcast collection not initialized for message ${msgId}`
                );
            } else {
                // Use retry helper without sentWaba fallback for incoming path
                const result = await findTemplateCodeWithRetry(msgId, {
                    includeSentWabaFallback: false,
                    attempts: 4,
                    initialDelayMs: 1500,
                    factor: 3,
                });

                if (result && result.template_code) {
                    template_code = result.template_code;
                    console.log(
                        `Template found for incoming message ${msgId} with retry: ${template_code}`
                    );
                    sendDiscordMessage(
                        "Template Found - Incoming",
                        `✅ Template found for incoming message with retry\nMessage ID: ${msgId}\nTemplate ID: ${template_code}\nFrom: ${to}\nTo: ${from}\nMessage Type: ${msgType}\nSource: ${result.source}`
                    );
                } else {
                    sendDiscordMessage(
                        "No Template Found After Retries - Incoming",
                        `❌ No template found for incoming message after retries\nMessage ID: ${msgId}\nFrom: ${to}\nTo: ${from}\nMessage Type: ${msgType}`
                    );
                }
            }
        } catch (error) {
            console.error(
                "Error checking broadcast collection for template:",
                error
            );

            // Log template lookup errors for incoming message
            sendDiscordMessage(
                "Template Lookup Error - Incoming",
                `🚨 Error checking broadcast collection for incoming message\nMessage ID: ${msgId}\nError: ${error.message}\nStack: ${error.stack}`
            );
        }
    }

    // Log final message processing result for incoming message
    const finalIncomingMessage = template_code
        ? `%%%template%%% ${template_code}`
        : msg;
    sendDiscordMessage(
        "Message Processing Complete - Incoming",
        `📝 Incoming message processing complete\nMessage ID: ${msgId}\nTemplate Applied: ${
            template_code ? "Yes" : "No"
        }\nTemplate Code: ${
            template_code || "None"
        }\nFinal Message: ${finalIncomingMessage}\nOriginal Message: ${msg}\nMessage Type: ${msgType}\nFrom: ${to}\nTo: ${from}`
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
                    Message: template_code
                        ? `%%%template%%% ${template_code}`
                        : msg,
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
    console.log("🔍 detectingAndModifyingDataFormat called with:", JSON.stringify(eventData, null, 2));
    
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

const mainEngine = async (req, res) => {
    try {
        console.log("🚀 mainEngine started");
        const startTime = Date.now();
        const extractedData = extractEventData(req);
        console.log("📥 Extracted data:", JSON.stringify(extractedData, null, 2));
        
        if (!extractedData.workspace_id) {
            // sendDiscordMessage("Extracted Data", JSON.stringify(extractedData));
        }
        let eventData = await detectingAndModifyingDataFormat(extractedData);
        console.log("🔄 Event data after format detection:", JSON.stringify(eventData, null, 2));
        
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
            const errorMsg = `Backup prerequisites not met:\nOrg ID: ${
                eventData.orgId
            }\nPhone: ${eventData.phoneNumber}\nChatters: ${
                Object.keys(eventData.chatters).length
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


        let startTimeLastMsgOne = Date.now();

        let lastMessageOfChatters = await mongoConversationCollection
            .find({
                from: eventData.phoneNumber,
                to: { $in: toPhoneNumberArray },
                projection: {
                    _id: 0,
                    to: 1,
                    last_message_time: 1,
                },
            })
            .toArray();
        

        let lastMessageOfChattersMap = {};
        for (let message of lastMessageOfChatters) {
            lastMessageOfChattersMap[message.to] = message.last_message_time;
        }

        for (let chatter in eventData.chatters) {
            let getLastMessageTime =
                lastMessageOfChattersMap[chatter.split("@")[0]];

            for (let chat of eventData.chatters[chatter]) {
                let chatDate = chat.Date;
                if (getLastMessageTime && chat.Datetime <= getLastMessageTime) {
                    continue;
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
            }
        }

        // Process data for BigQuery after dateAccChats is created
        try {
            console.log("🚀 Starting BigQuery processing...");
            const bigQueryResult = await bigQueryProcessor(dateAccChats, eventData.orgId, eventData.uid ?? eventData.workspace_id ?? null);
            console.log(`✅ BigQuery processing completed: ${bigQueryResult}`);
            
            // Log successful BigQuery processing
            await sendDiscordMessage(
                "BigQuery Processing Success",
                `✅ Successfully processed data for BigQuery\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nResult: ${bigQueryResult}`
            );
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
            const filePath = `${eventData.orgId}/${chatKey.split("-")[0]}/${
                chatKey.split("-")[1]
            }/${chatKey.split("-")[2]}/${eventData.phoneNumber}.json`;

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
                    await file.save(JSON.stringify(fileData, null, 2), {
                        contentType: "application/json",
                        resumable: false,
                    });

                    // Log successful file creation
                    // await sendDiscordMessage(
                    //     "GCS File Creation Success",
                    //     `✅ Successfully created GCS file\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nDate: ${chatKey}`
                    // );
                } catch (uploadError) {
                    console.error(
                        `GCS upload error for ${filePath}:`,
                        uploadError
                    );

                    // Log upload error
                    await sendDiscordMessage(
                        "GCS Upload Error",
                        `❌ GCS upload failed\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${uploadError.message}`
                    );

                    try {
                        await file.save(JSON.stringify(fileData, null, 2), {
                            contentType: "application/json",
                            resumable: true,
                        });

                        // Log successful retry
                        await sendDiscordMessage(
                            "GCS Retry Success",
                            `🔄 GCS retry upload successful\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}`
                        );
                    } catch (retryError) {
                        console.error(
                            `GCS retry upload failed for ${filePath}:`,
                            retryError
                        );

                        // Log retry failure
                        await sendDiscordMessage(
                            "GCS Retry Failed",
                            `💥 GCS retry upload failed\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${retryError.message}\nRetry: ${retryError}`
                        );

                        throw retryError; // Re-throw to be caught by outer error handler
                    }
                }
                continue;
            }

            const [buffer] = await file.download();

            try {
                const fileContent = buffer.toString("utf8");
                let existingContent = JSON.parse(fileContent);

                for (let name in eventData.nameMapping) {
                    if (!existingContent.nameMapping[name]) {
                        existingContent.nameMapping[name] = "";
                    }
                    if (eventData.nameMapping[name]) {
                        existingContent.nameMapping[name] =
                            eventData.nameMapping[name];
                    }
                }

                for (let chatter in dateAccChats[chatKey]) {
                    dateAccChats[chatKey][chatter] = dateAccChats[chatKey][
                        chatter
                    ].filter((chat) => {
                        const messageId = chat.MessageId;
                        if (messageId) {
                            const messageIdPattern = new RegExp(
                                `"${messageId}"`,
                                "i"
                            );
                            return !messageIdPattern.test(fileContent);
                        }
                        return true;
                    });

                    if (!existingContent.chatters.hasOwnProperty(chatter)) {
                        existingContent.chatters[chatter] = [];
                    }

                    existingContent.chatters[chatter] = [
                        ...existingContent.chatters[chatter],
                        ...dateAccChats[chatKey][chatter],
                    ];
                }

                try {
                    await file.save(JSON.stringify(existingContent, null, 2), {
                        contentType: "application/json",
                        resumable: false,
                    });

                    
                } catch (uploadError) {
                    console.error(
                        `GCS update error for ${filePath}:`,
                        uploadError
                    );

                    // Log update error
                    await sendDiscordMessage(
                        "GCS Update Error",
                        `❌ GCS update failed\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${uploadError.message}`
                    );

                    try {
                        await file.save(
                            JSON.stringify(existingContent, null, 2),
                            {
                                contentType: "application/json",
                                resumable: true,
                            }
                        );

                        // Log successful retry
                        await sendDiscordMessage(
                            "GCS Update Retry Success",
                            `🔄 GCS update retry successful\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}`
                        );
                    } catch (retryError) {
                        console.error(
                            `GCS retry update failed for ${filePath}:`,
                            retryError
                        );

                        // Log retry failure
                        await sendDiscordMessage(
                            "GCS Update Retry Failed",
                            `💥 GCS update retry failed\nPath: ${filePath}\nOrg ID: ${eventData.orgId}\nPhone: ${eventData.phoneNumber}\nError: ${retryError.message}`
                        );

                        throw retryError; // Re-throw to be caught by outer error handler
                    }
                }
            } catch (error) {
                console.log("error -------------------------- start");
                console.log(error);
                console.log("error -------------------------- end");
                return false;
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

        // Final backup validation
        try {
            const totalMessages = Object.values(dateAccChats).reduce(
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

            
        } catch (validationError) {
            console.error("Error during backup validation:", validationError);
            await sendDiscordMessage(
                "Backup Validation Error",
                `❌ Error during backup validation\nError: ${validationError.message}`
            );
        }

        console.log(`------ REQUEST COMPLETED : ${count++} ------`);

        return true;
    } catch (error) {
        console.error("Error processing webhook (inner):", error);

        // Send comprehensive error notification
        await sendDiscordMessage(
            "Backup Process Failed",
            `💥 Backup process failed\nOrg ID: ${
                eventData?.orgId || "Unknown"
            }\nPhone: ${eventData?.phoneNumber || "Unknown"}\nError: ${
                error.message
            }\nStack: ${error.stack}\nTime: ${Date.now() - startTime}ms`
        );

        return false;
    }
};

let count = 0;
const webhookProcessor = async (req, res) => {
    // const webhookProcessor = async (req, res) => {
    try {
        console.log("🎯 webhookProcessor called with request body:", JSON.stringify(req.body, null, 2));
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




app.post("/", async (req, res) => {
    return (await webhookProcessor(req, res))
});

app.listen(3003, () => {
    console.log("Server is listening on PORT =>", 3003);
});

// Export for external use
exports.webhookProcessor = webhookProcessor;