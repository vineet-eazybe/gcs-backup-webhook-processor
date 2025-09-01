const express = require("express");
const { Storage } = require("@google-cloud/storage");
const { MongoClient } = require("mongodb"); // Using JS Date, Timestamp not strictly needed here
const axios = require("axios");

const app = express();

// Use JSON middleware to parse request bodies with increased size limit
app.use(express.json({ limit: "100mb" }));

const MONGODB_URI =
    "mongodb+srv://eazybe-backend:2u14oH9wSlBdOBXz@chat-backup-us-central1.ryikyh.mongodb.net/?retryWrites=true&w=majority&appName=chat-backup-us-central1";

const MONGODB_DB = "eazy-be";
const MONGO_CONVERSATION_COLLECTION = "backup_last_messages"; // Collection for summaries
const MONGO_MESSAGES_COLLECTION = "sent_waba_messages";
const MONGO_PUB_SUB_MESSAGES_COLLECTION = "pub_sub_messages";
const GCS_BUCKET = "meta-webhooks"; // Bucket for detailed logs
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
    // projectId: "waba-454907",
    // keyFilename: "my-key.json",
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
let mongoConversationCollection;
let mongoSentMsgCollection;
let mongoPubSubMessagesCollection;

// --- Connect to MongoDB ---
async function connectToMongo() {
    try {
        if (mongoClient?.topology && mongoClient.topology.isConnected()) {
            return true;
        }
        await mongoClient.connect({
            maxPoolSize: 10,
            minPoolSize: 0,
            serverSelectionTimeoutMS: 5000,
            socketTimeoutMS: 45000,
        });
        const db = mongoClient.db(MONGODB_DB);
        mongoConversationCollection = db.collection(
            MONGO_CONVERSATION_COLLECTION
        );
        mongoSentMsgCollection = db.collection(MONGO_MESSAGES_COLLECTION);
        mongoPubSubMessagesCollection = db.collection(
            MONGO_PUB_SUB_MESSAGES_COLLECTION
        );
        console.log(
            `Connected to MongoDB, using database '${MONGODB_DB}' and collection '${MONGO_CONVERSATION_COLLECTION}'.`
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

// --- Helper Functions ---

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

    let sentMsg = await mongoSentMsgCollection.findOne({
        msgId: msgId,
    });

    if (!sentMsg) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
        sentMsg = await mongoSentMsgCollection.findOne({
            msgId: msgId,
        });
        if (!sentMsg) {
            return null;
        }
    }

    let from = metadata.display_phone_number;
    let to = status.recipient_id;

    if (!from || !to || !msgId || !msgTimestamp) {
        return NaN;
    }

    let statusValue = status.status;
    if (statusValue !== "delivered" && statusValue !== "sent") {
        return null;
    }

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

    if (!org_id) return 0;

    let isBroadcast = sentMsg.isBroadcast || false;
    let pricing = null;

    if (status.pricing && status.pricing.category) {
        pricing = {
            category: status.pricing.category,
            billable: status.pricing.billable,
            pricing_model: status.pricing.pricing_model,
        };
    }

    let formattedObj = {
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
                    Message:
                        sentMsg.type === "text"
                            ? sentMsg.payload
                            : sentMsg.type === "template"
                            ? `%%%template%%% ${sentMsg.payload.templateName}`
                            : "",
                    Chatid: `${to}@c.us`,
                    File: null,
                    Ack: 1,
                    Datetime: msgTimestamp,
                    Date: new Date(msgTimestamp).toISOString().split("T")[0],
                    Direction: direction,
                    Sentbyid: `${from}@c.us`,
                    CreatedByUser: from,
                    SentByNumber: from,
                    SpecialData: sentMsg.payload || {},
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

    if (!org_id) return 0;

    let formattedObj = {
        orgId: org_id,
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
                    SpecialData: specialData,
                },
            ],
        },
        groupParticipants: {},
    };

    return formattedObj;
}

async function detectingAndModifyingDataFormat(eventData) {
    if (
        // eventData?.mode && eventData?.mode === "EXT" &&
        eventData?.orgId &&
        eventData?.phoneNumber &&
        eventData?.nameMapping &&
        eventData?.chatters
        // eventData?.groupParticipants
    ) {
        return eventData;
    }

    console.log(
        "------------****************> extractedData 1start <---------------------------"
    );
    console.log(JSON.stringify(eventData));
    console.log(
        "------------------------> extractedData 1end <****************---------------"
    );

    let sendFormatResult = await checkForSendFormat(eventData);
    console.log(JSON.stringify(sendFormatResult));
    console.log("--> sendFormatResult");
    if (sendFormatResult) {
        return sendFormatResult;
    }

    let replyFormatResult = await checkForReplyFormat(eventData);
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

const url = ["https://webhook.site/webhook-processor-logs"];

const sendDiscordMessage = async (
    title = "Pending Message Monitor",
    formattedMessage
) => {
    try {
        // const randomIndex = Math.floor(Math.random() * url.length);
        // const webhookUrl = url[randomIndex];
        // const response = await axios.post(
        //     webhookUrl,
        //     {
        //         content: `CLOUD : ${formattedMessage}`,
        //         username: title,
        //         avatar_url: null,
        //     },
        //     {
        //         timeout: 5000, // 5 second timeout
        //         headers: {
        //             "Content-Type": "application/json",
        //         },
        //     }
        // );
    } catch (error) {
        // console.error("Error sending Discord message:",);
    }
};

const checkForWABAExistance = async (phoneNumber) => {
    try {
        const response = await axios.get(
            "https://api.eazybe.com/v2/waba/check-phone-number",
            { params: { phoneNumber } }
        );
        return Boolean(response?.data?.data?.data?.isWabaConnected);
    } catch (error) {
        return false;
    }
};

const mainEngine = async (req, res) => {
    try {
        const extractedData = extractEventData(req);
        let eventData = await detectingAndModifyingDataFormat(extractedData);
        if (eventData?.workspace_id) {
            const needBackup = await checkForWABAExistance(
                eventData?.phoneNumber
            );
            if (needBackup) {
                try {
                    const currentDate = new Date().toISOString();

                    axios.post(
                        "https://api.eazybe.com/v2/users/last-chat-sync-time",
                        {
                            workspace_id: Number(eventData.workspace_id),
                            last_chat_sync_time: currentDate,
                        },
                        {
                            headers: {
                                "private-key": "123456789",
                                "Content-Type": "application/json",
                            },
                        }
                    );

                    console.log(
                        `Updated last chat sync time for workspace_id: ${eventData.workspace_id}`
                    );
                } catch (apiError) {
                    console.error(
                        "Error updating last chat sync time:",
                        apiError.message
                    );
                }
                return true;
            }
        }
        let pubsubMessage = await mongoPubSubMessagesCollection.findOne({
            messageId: req?.body?.message?.messageId,
        });
        if (pubsubMessage) {
            if (pubsubMessage.status === "processing") {
                updatePubSubMessageStatus(messageId, "processing");
                return "processing";
            }
            if (pubsubMessage.status === "completed") {
                updatePubSubMessageStatus(messageId, "completed");
                return true;
            }
            if (pubsubMessage.status === "error") {
                updatePubSubMessageStatus(messageId, "error");
                return false;
            }
        }

        await mongoPubSubMessagesCollection.insertOne({
            messageId: req?.body?.message?.messageId,
            phoneNumber: eventData?.phoneNumber || null,
            publishTime: req?.body?.message?.publishTime,
            status: "processing",
            calls: 0,
            timeTaken: null,
            createdAt: new Date(),
        });

        if (!eventData || typeof eventData !== "object") {
            sendDiscordMessage(
                "Event Data Type Error",
                `Extracted event data is not a valid object: ${JSON.stringify(
                    eventData
                )}`
            );
            return false;
        }

        if (!eventData?.orgId) {
            return false;
        }
        if (!eventData?.phoneNumber) {
            return false;
        }
        if (!eventData?.nameMapping) {
            return false;
        }
        if (!eventData?.chatters) {
            return false;
        }
        if (!eventData?.groupParticipants) {
            return false;
        }

        let dateAccChats = {};
        let toPhoneNumberArray = Object.keys(eventData.chatters).map(
            (chatter) => chatter.split("@")[0]
        );
        console.log("Total chatter =>", toPhoneNumberArray.length);

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
        sendDiscordMessage(
            "Time taken for all Last Message",
            `Time taken for last message of chatters in MongoDB: ${
                Date.now() - startTimeLastMsgOne
            }ms`
        );

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
        // sendDiscordMessage("Payload",JSON.stringify(eventData));

        let startTimeLastMsg = Date.now();
        await createLastMessages(
            eventData?.orgId,
            eventData?.phoneNumber,
            eventData.chatters,
            eventData.nameMapping,
            eventData?.profileImages
        );
        let timeTakenLastMsg = Date.now() - startTimeLastMsg;
        sendDiscordMessage(
            "Time taken Last Message",
            `Time taken for last message of chatters in MongoDB: ${timeTakenLastMsg}ms`
        );

        for (let chatKey in dateAccChats) {
            const filePath = `${eventData.orgId}/${chatKey.split("-")[0]}/${
                chatKey.split("-")[1]
            }/${chatKey.split("-")[2]}/${eventData.phoneNumber}.json`;

            const file = await bucket.file(filePath);
            let [fileExists] = await file.exists();

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
                } catch (uploadError) {
                    console.error(
                        `GCS upload error for ${filePath}:`,
                        uploadError
                    );
                    try {
                        await file.save(JSON.stringify(fileData, null, 2), {
                            contentType: "application/json",
                            resumable: true,
                        });
                    } catch (retryError) {
                        console.error(
                            `GCS retry upload failed for ${filePath}:`,
                            retryError
                        );
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
                    try {
                        await file.save(
                            JSON.stringify(existingContent, null, 2),
                            {
                                contentType: "application/json",
                                resumable: true,
                            }
                        );
                    } catch (retryError) {
                        console.error(
                            `GCS retry update failed for ${filePath}:`,
                            retryError
                        );
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

                axios.post(
                    "https://api.eazybe.com/v2/users/last-chat-sync-time",
                    {
                        workspace_id: Number(eventData.workspace_id),
                        last_chat_sync_time: currentDate,
                    },
                    {
                        headers: {
                            "private-key": "123456789",
                            "Content-Type": "application/json",
                        },
                    }
                );

                console.log(
                    `Updated last chat sync time for workspace_id: ${eventData.workspace_id}`
                );
            } catch (apiError) {
                console.error(
                    "Error updating last chat sync time:",
                    apiError.message
                );
            }
        }

        console.log(`------ REQUEST COMPLETED : ${count++} ------`);

        return true;
    } catch (error) {
        console.error("Error processing webhook (inner):", error);
        return false;
    }
};

let count = 0;
exports.webhookProcessor = async (req, res) => {
    // const webhookProcessor = async (req, res) => {
    try {
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
                    sendDiscordMessage(
                        "Time taken",
                        `Time taken: ${(Date.now() - startTime) / 1000}s`
                    );
                } else {
                    throw new Error(
                        "Unacknowledged event with messageId: " +
                            req?.body?.message?.messageId
                    );
                }
            })
            .catch((error) => {
                sendDiscordMessage(
                    "Error",
                    `Error in webhook processor: ${error}`
                );
                updatePubSubMessageStatus(
                    req?.body?.message?.messageId,
                    "error"
                );
            });
        sendDiscordMessage("Total calls", ++count);
        return res.status(200).send("Event Captured");
    } catch (error) {
        sendDiscordMessage(
            "Error",
            `Error processing webhook (outer): ${error}`
        );
        count++;
        return res.status(500).send("Error Occurred");
    }
};
