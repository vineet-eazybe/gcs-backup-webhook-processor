const { BigQuery } = require("@google-cloud/bigquery");
const { v4: uuidv4 } = require('uuid');
const bigquery = new BigQuery();
const datasetId = "whatsapp_analytics"; // your dataset
const tableId = "message_events";       // your table

/**
 * Transforms a single chat object from the source data into the format 
 * required for the BigQuery table schema.
 * @param {object} chat - The input chat object from dateAccChats.
 * @param {string} orgId - The organization ID for this batch.
 * @returns {object} The formatted row object for BigQuery.
 */
const transformChatToRow = (chat, orgId, uid, chatAgentMap = null) => {
  const messageText = chat.Message || null;

  // Ensure orgId is a string
  const orgIdStr = String(orgId);
  const eventId = `${orgIdStr}-${uuidv4()}`;
  
  // Validate required fields
  if (!chat.MessageId) {
    console.warn("⚠️ Missing MessageId in chat:", chat);
  }
  if (!chat.Chatid) {
    console.warn("⚠️ Missing Chatid in chat:", chat);
  }
  if (!chat.Datetime) {
    console.warn("⚠️ Missing Datetime in chat:", chat);
  }
  
  // Calculate agent_phone_number based on message direction
  let agentPhoneNumber = null;
  const chatId = chat.Chatid || `missing_${Date.now()}`;
  const direction = chat.Direction || 'UNKNOWN';
  const senderNumber = chat.SentByNumber || '123456789';
  
  // Extract recipient number from chatId (format: "phone@c.us")
  const recipientNumber = chatId ? chatId.split('@')[0] : null;
  
  if (direction === 'OUTGOING') {
    // For outgoing messages, the sender is the agent
    agentPhoneNumber = senderNumber;
    console.log(`📤 Outgoing message: Agent (sender) = ${agentPhoneNumber}`);
  } else if (direction === 'INCOMING') {
    // For incoming messages, the recipient is the agent
    // The recipient is the person receiving the message (the agent)
    agentPhoneNumber = recipientNumber;
    console.log(`📥 Incoming message: Agent (recipient) = ${agentPhoneNumber}`);
    
    if (!agentPhoneNumber) {
      console.warn(`⚠️ Could not determine agent phone number for incoming message in chat ${chatId}`);
      agentPhoneNumber = "12345"; // Default fallback
    }
  } else {
    console.warn(`⚠️ Unknown message direction: ${direction}`);
    agentPhoneNumber = senderNumber; // Default to sender
  }
  
  return {
    // REQUIRED fields
    event_id: eventId, // Generate a unique ID for each event
    message_id: chat.MessageId || `missing_${Date.now()}`,
    chat_id: chatId,
    org_id: orgIdStr, // Ensure org_id is a string
    message_timestamp: chat.Datetime ? new Date(chat.Datetime).toISOString() : new Date().toISOString(),
    ingestion_timestamp: new Date().toISOString(),
    direction: direction,
    type: chat.Type || 'unknown',

    // NULLABLE fields
    user_id: uid ? String(uid) : null,
    sender_number: senderNumber,
    ack: chat.Ack || null,
    message_text: messageText,
    
    // Media fields (from root and SpecialData)
    file_url: chat.File || null,

    // Broadcast fields
    is_broadcast: chat.isBroadcast || false,
    
    // Other nested data and calculated fields
    special_data: chat.SpecialData ? JSON.stringify(chat.SpecialData) : null,
    word_count: messageText ? messageText.trim().split(/\s+/).length : null,
    
    // NEW FIELD: Agent phone number
    agent_phone_number: agentPhoneNumber || "12345",
  };
};

/**
 * Flattens a nested chat data object and inserts the records into BigQuery.
 * @param {object} dateAccChats - The nested object containing chat data.
 * @param {string} orgId - The organization ID to associate with these records.
 */
exports.bigQueryProcessor = async (dateAccChats, orgId, uid) => {
  let rows = null; // Declare rows outside try block for error handling
  
  try {
    console.log("🔍 BigQuery processor starting...");
    console.log("📊 DateAccChats keys:", Object.keys(dateAccChats));
    
    // Flatten the nested object structure into a single array of chats
    const allChats = Object.values(dateAccChats).flatMap(chatterObj => Object.values(chatterObj).flat());
    console.log("📱 Total chats to process:", allChats.length);

    if (allChats.length === 0) {
      console.log("⚠️ No rows to insert into BigQuery");
      return "Success - No Data";
    }

    // Transform the array of chats into an array of BigQuery rows
    // Note: chatAgentMap is not currently implemented, so we pass null
    rows = allChats.map(chat => transformChatToRow(chat, orgId, uid, null));
    console.log("🔄 Transformed rows for BigQuery:", rows.length);

    console.log("📝 Inserting rows into BigQuery...");
    console.log("🔍 Sample row data:", JSON.stringify(rows[0], null, 2));
    
    const [insertResult] = await bigquery.dataset(datasetId).table(tableId).insert(rows);
    
    // Check for partial failures
    if (insertResult && insertResult.length > 0) {
      console.error("❌ BigQuery partial failure detected:");
      insertResult.forEach((error, index) => {
        console.error(`❌ Row ${index} error:`, JSON.stringify(error, null, 2));
      });
      throw new Error(`BigQuery partial failure: ${insertResult.length} rows failed`);
    }
    
    console.log(`✅ Successfully inserted ${rows.length} rows into BigQuery`);
    return "Success";
  } catch (error) {
    console.error("❌ BigQuery insert error:", JSON.stringify(error, null, 2));
    console.error("❌ Error details:", error.message);
    console.error("❌ Error stack:", error.stack);
    
    // Log sample data for debugging
    if (rows && rows.length > 0) {
      console.error("🔍 Sample row that failed:", JSON.stringify(rows[0], null, 2));
    }
    
    throw error; // Re-throw to be caught by the main error handler
  }
};