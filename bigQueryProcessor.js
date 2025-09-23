const { BigQuery } = require("@google-cloud/bigquery");
const { v4: uuidv4 } = require('uuid');
const bigquery = new BigQuery({
    keyFilename: "./gcp-key.json",
    projectId: "waba-454907",
});
const datasetId = "whatsapp_analytics"; // your dataset
const tableId = "message_events";       // your table

/**
 * Transforms a single chat object from the source data into the format 
 * required for the BigQuery table schema.
 * @param {object} chat - The input chat object from dateAccChats.
 * @param {string} orgId - The organization ID for this batch.
 * @returns {object} The formatted row object for BigQuery.
 */
const transformChatToRow = (chat, orgId, uid) => {
  const messageText = chat.Message || null;

  const eventId = `${orgId}-${uuidv4()}`;
  return {
    // REQUIRED fields
    event_id: eventId, // Generate a unique ID for each event
    message_id: chat.MessageId,
    chat_id: chat.Chatid,
    org_id: orgId, // Pass org_id from the function's context
    message_timestamp: new Date(chat.Datetime).toISOString(),
    ingestion_timestamp: new Date().toISOString(),
    direction: chat.Direction,
    type: chat.Type,

    // NULLABLE fields
    user_id: uid || null,
    sender_number: chat.SentByNumber || null,
    ack: chat.Ack || null,
    message_text: messageText,
    
    // Media fields (from root and SpecialData)
    file_url: chat.File || null,

    // Broadcast fields
    is_broadcast: chat.isBroadcast || null,
    
    // Other nested data and calculated fields
    special_data: chat.SpecialData ? JSON.stringify(chat.SpecialData) : null,
    word_count: messageText ? messageText.trim().split(/\s+/).length : null,
  };
};

/**
 * Flattens a nested chat data object and inserts the records into BigQuery.
 * @param {object} dateAccChats - The nested object containing chat data.
 * @param {string} orgId - The organization ID to associate with these records.
 */
exports.bigQueryProcessor = async (dateAccChats, orgId, uid) => {
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
    const rows = allChats.map(chat => transformChatToRow(chat, orgId, uid));
    console.log("🔄 Transformed rows for BigQuery:", rows.length);

    console.log("📝 Inserting rows into BigQuery...");
    await bigquery.dataset(datasetId).table(tableId).insert(rows);
    console.log(`✅ Successfully inserted ${rows.length} rows into BigQuery`);
    
    return "Success";
  } catch (error) {
    console.error("❌ BigQuery insert error:", JSON.stringify(error, null, 2));
    console.error("❌ Error details:", error.message);
    console.error("❌ Error stack:", error.stack);
    throw error; // Re-throw to be caught by the main error handler
  }
};