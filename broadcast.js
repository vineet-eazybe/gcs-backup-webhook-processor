 const createBulkBroadcastv2 = async (workspace_id, sendBroadcastData) => {
    try {
      let getEmbeddedSignUpInfo = await this.wabaService.getSignUpEmbedByWorkspaceId(workspace_id);

      if (!getEmbeddedSignUpInfo?.access_token || !getEmbeddedSignUpInfo?.phone_number_id || !getEmbeddedSignUpInfo?.phone_number) {
        throw new BadRequestException({ reason: "User is not signed up yet.", balanceError: false });
      }

      // Validate template name and language before proceeding
      await this.assertValidTemplate(
        getEmbeddedSignUpInfo.access_token,
        getEmbeddedSignUpInfo.waba_id,
        sendBroadcastData.templateName,
        sendBroadcastData.templateLanguage,
      );

      // Require at least one contact selected
      if (!sendBroadcastData?.data || sendBroadcastData.data.length === 0) {
        throw new BadRequestException({ reason: "Please select at least one contact to send broadcast", balanceError: false });
      }

      if(sendBroadcastData?.broadcastName){
        const existingBroadcastWithSameName = await this.db.collection('broadcasts').findOne({
          internal_user_id: workspace_id,
          broadCastName: sendBroadcastData?.broadcastName
        });
        
        if (existingBroadcastWithSameName) {
          throw new BadRequestException({ reason: "Broadcast with same name exists", balanceError: false });
        }
      }

      let org_id = (await this.UserMappingRepository.findOne({ where: { user_id: workspace_id } }))?.dataValues?.org_id;
      let orgPlanInfo = (await this.OrgCreditRepository.findOne({ where: { org_id: Number(org_id) } }))?.dataValues;

      // Check subscription
      let subscription = "";
      if (orgPlanInfo.plan_id > 9 && (new Date(orgPlanInfo.validity)) > new Date()) {
        subscription = "team";
      }

      if (!subscription) {
        let individualPlanInfo = (await this.creditRepository.findOne({ where: { workspace_id } }))?.dataValues;
        if ((individualPlanInfo.plan_id === 8 || individualPlanInfo.plan_id === 9) && (new Date(individualPlanInfo.expiry_date)) > (new Date())) {
          subscription = "plus";
        }
      }

      if (!subscription) {
        throw new BadRequestException({ reason: "No subscriptions available.", balanceError: false });
      }

      const getBalance = await this.ledgerService.getWalletBalance(Number(org_id));

      // Create a dummy record to prevent duplicate broadcast names
      const campaign_id = randomStringGenerator(20);
      const dummyRecord = {
        internal_user_id: workspace_id,
        broadCastName: sendBroadcastData?.broadcastName,
        campaign_id: campaign_id,
        status: 'PENDING',
        created_at: new Date(),
        updated_at: new Date()
      };

      try {
        await this.db.collection('broadcasts').insertOne(dummyRecord);
      } catch (error) {
        if (error.code === 11000) { // MongoDB duplicate key error
          throw new BadRequestException({ reason: "Broadcast with same name exists", balanceError: false });
        }
        throw error;
      }

      // Calculate total cost and validate phone numbers
      let totalCost = 0;
      const processedData = [];
      let failedCount = 0;
      let serialCounter = 1;

      for (let obj of sendBroadcastData.data) {
        // Normalize and validate phone numbers
        const normalized = this.normalizePhoneNumber(obj.countryCode, obj.phoneNumber);
        if (!normalized) {
          // Create a failed broadcast document for invalid phone numbers
          const failedDocument = {
            internal_user_id: workspace_id,
            eazybe_org_id: org_id,
            facebook_business_manager_id: getEmbeddedSignUpInfo?.businessId || "",
            whatsapp_business_account_id: getEmbeddedSignUpInfo?.waba_id || "",
            broadCastName: sendBroadcastData?.broadcastName || "",
            campaign_id: campaign_id,
            channel_id: 'broadcast_campaign',
            api_used: 'cloud',
            event_timestamp: null,
            message_sent_timestamp: Date.now(),
            message_direction: 'outgoing_api',
            template_id: sendBroadcastData.templateName,
            recipient_phone_number: `${obj.countryCode}${obj.phoneNumber}`,
            dispatch_status_to_whatsapp: "FAILED_TO_SEND",
            delivery_status_from_whatsapp: "failed",
            failure_reason_code: "INVALID_PHONE_NUMBER",
            failure_reason_text: `Invalid phone number format for ${obj.countryCode}${obj.phoneNumber}`,
            whatsapp_conversation_type: sendBroadcastData?.templateType?.toUpperCase(),
            cost_incurred_from_bsp_currency: '',
            cost_incurred_from_bsp_amount: null,
            user_charge_logic_version: 'v1',
            amount_to_deduct_from_user_wallet_currency: getBalance?.currency,
            amount_to_deduct_from_user_wallet_amount: 0,
            user_wallet_deduction_id: null,
            user_wallet_deduction_status: "DEDUCTION_FAILED",
            total_broadcast_to_send: sendBroadcastData.data.length,
            serial_of_broadcast: serialCounter,
          };

          // Insert the failed broadcast document
          await this.db.collection('broadcasts').insertOne(failedDocument);
          failedCount++;
          serialCounter++;
          
          // Continue to next phone number
          continue;
        }

        const processedObj = { ...obj, phoneNumber: normalized.local };
        let broadcastPrice = null;

        // Get pricing based on subscription and currency
        if (getBalance.currency === "INR") {
          if (subscription === "plus") {
            let priceInfo = await this.WabaPricingPlusPlanInrRepository.findOne({
              where: { country_codes: obj.countryCode }
            });
            if (priceInfo) {
              broadcastPrice = Number(priceInfo.dataValues[(sendBroadcastData.templateType).toLowerCase()])?.toFixed(5);
            }
          } else if (subscription === "team") {
            let priceInfo = await this.WabaPricingTeamAssistPlanInrRepository.findOne({
              where: { country_codes: obj.countryCode }
            });
            if (priceInfo) {
              broadcastPrice = Number(priceInfo.dataValues[(sendBroadcastData.templateType).toLowerCase()])?.toFixed(5);
            }
          }
        } else {
          if (subscription === "plus") {
            let priceInfo = await this.WabaPricingPlusPlanUsdRepository.findOne({
              where: { country_codes: obj.countryCode }
            });
            if (priceInfo) {
              broadcastPrice = Number(priceInfo.dataValues[(sendBroadcastData.templateType).toLowerCase()])?.toFixed(5);
            }
          } else if (subscription === "team") {
            let priceInfo = await this.WabaPricingTeamAssistPlanUsdRepository.findOne({
              where: { country_codes: obj.countryCode }
            });
            if (priceInfo) {
              broadcastPrice = Number(priceInfo.dataValues[(sendBroadcastData.templateType).toLowerCase()])?.toFixed(5);
            }
          }
        }

        if (broadcastPrice === null) {
          // Create a failed broadcast document for unsupported country codes
          const failedDocument = {
            internal_user_id: workspace_id,
            eazybe_org_id: org_id,
            facebook_business_manager_id: getEmbeddedSignUpInfo?.businessId || "",
            whatsapp_business_account_id: getEmbeddedSignUpInfo?.waba_id || "",
            broadCastName: sendBroadcastData?.broadcastName || "",
            campaign_id: campaign_id,
            channel_id: 'broadcast_campaign',
            api_used: 'cloud',
            event_timestamp: null,
            message_sent_timestamp: Date.now(),
            message_direction: 'outgoing_api',
            template_id: sendBroadcastData.templateName,
            recipient_phone_number: `${obj.countryCode}${obj.phoneNumber}`,
            dispatch_status_to_whatsapp: "FAILED_TO_SEND",
            delivery_status_from_whatsapp: "failed",
            failure_reason_code: "UNSUPPORTED_COUNTRY_CODE",
            failure_reason_text: `Cannot send broadcast to ${obj.countryCode}${obj.phoneNumber} - pricing not available`,
            whatsapp_conversation_type: sendBroadcastData?.templateType?.toUpperCase(),
            cost_incurred_from_bsp_currency: '',
            cost_incurred_from_bsp_amount: null,
            user_charge_logic_version: 'v1',
            amount_to_deduct_from_user_wallet_currency: getBalance?.currency,
            amount_to_deduct_from_user_wallet_amount: 0,
            user_wallet_deduction_id: null,
            user_wallet_deduction_status: "DEDUCTION_FAILED",
            total_broadcast_to_send: sendBroadcastData.data.length,
            serial_of_broadcast: serialCounter,
          };

          // Insert the failed broadcast document
          await this.db.collection('broadcasts').insertOne(failedDocument);
          failedCount++;
          serialCounter++;
          
          // Continue to next phone number
          continue;
        }

        processedObj.price = Number(broadcastPrice);
        totalCost = totalCost + processedObj.price;
        processedData.push(processedObj);
      }

      // Check balance
      if (totalCost > getBalance.balance) {
        throw new BadRequestException({ reason: "Insufficient credits to send broadcast.", balanceError: true });
      }

      // Process broadcasts in background (don't await this)
      this.processBroadcastInBackground(
        workspace_id,
        org_id,
        campaign_id,
        processedData,
        sendBroadcastData,
        getEmbeddedSignUpInfo,
        getBalance,
        serialCounter
      ).catch(error => {
        console.error('Background broadcast processing failed:', error);
      });

      // Return immediately
      return {
        status: true,
        message: "Broadcast is being processed",
        data: {
          total_contacts: processedData.length,
          failed_contacts: failedCount,
          estimated_cost: totalCost,
          oldBalance: getBalance.balance,
          newEstimatedBalance: getBalance.balance - totalCost
        }
      };

    } catch (error) {
      console.error(error);
      if (error instanceof BadRequestException) {
        throw error;
      }
      return { status: false, message: "Broadcast creation failed", error: error };
    }
  }

  // Background processing method
  const processBroadcastInBackground = async (
    workspace_id,
    org_id,
    campaign_id,
    processedData,
    sendBroadcastData,
    getEmbeddedSignUpInfo,
    getBalance,
    startSerialCounter = 1
  ) =>{
    const failToSend = [];
    const broadcastDocuments = [];
    let count = startSerialCounter;
    try {
    await this.db.collection('broadcasts').deleteOne({
      internal_user_id: workspace_id,
        broadCastName: sendBroadcastData?.broadcastName,
        status: 'PENDING'
      });
    } catch (error) {
      console.log(`Error in deleting dummy record: ${error}`);
    }
    
    try {
      for (let obj of processedData) {
        // Step 1: Pre-create the record with PENDING_SEND status
        const document = {
          internal_user_id: workspace_id,
          whatsapp_message_id: null, // Will be updated after API call
          eazybe_org_id: org_id,
          facebook_business_manager_id: getEmbeddedSignUpInfo?.businessId || "",
          whatsapp_business_account_id: getEmbeddedSignUpInfo?.waba_id || "",
          broadCastName: sendBroadcastData?.broadcastName || "",
          campaign_id: campaign_id,
          channel_id: 'broadcast_campaign',
          api_used: 'cloud',
          event_timestamp: null,
          message_sent_timestamp: Date.now(),
          message_direction: 'outgoing_api',
          template_id: sendBroadcastData.templateName,
          template_name: sendBroadcastData.templateName,
          template_language: sendBroadcastData.templateLanguage,
          recipient_phone_number: `${obj.countryCode}${obj.phoneNumber}`,
          dispatch_status_to_whatsapp: "PENDING_SEND",
          delivery_status_from_whatsapp: null,
          failure_reason_code: null,
          failure_reason_text: '',
          whatsapp_conversation_type: sendBroadcastData?.templateType?.toUpperCase(),
          cost_incurred_from_bsp_currency: '',
          cost_incurred_from_bsp_amount: null,
          user_charge_logic_version: 'v1',
          amount_to_deduct_from_user_wallet_currency: getBalance?.currency,
          amount_to_deduct_from_user_wallet_amount: obj.price,
          user_wallet_deduction_id: null,
          user_wallet_deduction_status: "PENDING_DEDUCTION",
          total_broadcast_to_send: sendBroadcastData.data.length,
          serial_of_broadcast: count,
          created_at: new Date(),
          updated_at: new Date(),
          processing_status: "PENDING_SEND"
        };

        // Insert record BEFORE sending message
        const insertResult = await this.db.collection('broadcasts').insertOne(document);
        const recordId = insertResult.insertedId;

        // Step 2: Send the message
        let sendingStatus = await this.sendMessageTemplate(
          getEmbeddedSignUpInfo.waba_id,
          `${obj.countryCode}${obj.phoneNumber}`,
          { name: sendBroadcastData.templateName, language: sendBroadcastData.templateLanguage },
          getEmbeddedSignUpInfo.phone_number_id
        );

        // Step 3: Update the record with actual results
        const broadcastMessageId = sendingStatus ? sendingStatus?.messages?.[0]?.id : null;
        let ledgerData = null;

        if (sendingStatus.error) {
          failToSend.push({
            obj,
            reason: sendingStatus.error.error?.message || sendingStatus.error.error.toString() || 'Unknown error occurred'
          });
        } else {
          ledgerData = await this.ledgerService.deductCredits(
            workspace_id,
            Number(org_id),
            obj.price,
            { description: "Broadcast Charge", category: "API_USAGE" },
            getBalance?.currency.toUpperCase(),
            "broadcast"
          );
        }

        // Update the record with actual results
        const updateData = {
          whatsapp_message_id: broadcastMessageId,
          dispatch_status_to_whatsapp: sendingStatus.error ? "FAILED_TO_SEND" : "SENT_TO_BSP",
          delivery_status_from_whatsapp: sendingStatus.error ? "failed" : null,
          failure_reason_code: sendingStatus.error ? "TEMPLATE_SEND_FAILED" : null,
          failure_reason_text: sendingStatus?.error ? (sendingStatus.error.error?.message || '') : '',
          processing_status: sendingStatus.error ? "SEND_FAILED" : "PENDING_WEBHOOK",
          amount_to_deduct_from_user_wallet_amount: sendingStatus.error ? 0 : obj.price,
          user_wallet_deduction_id: ledgerData ? ledgerData?.mediciJournal?._id : null,
          user_wallet_deduction_status: ledgerData ? "DEDUCTION_SUCCESSFUL" : "DEDUCTION_FAILED",
          updated_at: new Date()
        };

        await this.db.collection('broadcasts').updateOne(
          { _id: recordId },
          { $set: updateData }
        );
        broadcastDocuments.push(document);

        if (!sendingStatus.error) {
          // Fix: Ensure consistent message ID field usage
          const sentMessageId = sendingStatus?.messages?.[0]?.id || null;
          let sentWabaMessage = {
            msgId: sentMessageId,
            from: getEmbeddedSignUpInfo.phone_number || "",
            to: `${obj.countryCode}${obj.phoneNumber}`,
            isBroadcast: true,
            type: "template",
            text: "",
            template_id: sendBroadcastData.templateName,
            template_name: sendBroadcastData.templateName,
            template_language: sendBroadcastData.templateLanguage,
            last_message: `%%%template%%% ${sendBroadcastData.templateName}`,
            createdAt: new Date(),
            updatedAt: new Date(),
            payload: {
              templateName: sendBroadcastData.templateName,
              templateLanguage: sendBroadcastData.templateLanguage,
              templateType: sendBroadcastData.templateType
            }
          };

          await this.db.collection('sent_waba_messages').insertOne(sentWabaMessage);
        }

        count++;

        // Add delay between messages to avoid rate limiting
        await new Promise((resolve) => { setTimeout(() => { resolve(true) }, 200) });
      }

      // Update the dummy record with actual broadcast data
      

      console.log(`Broadcast ${campaign_id} completed. Sent: ${broadcastDocuments.filter(d => d.dispatch_status_to_whatsapp === "SENT_TO_BSP").length}, Failed: ${failToSend.length}`);

    } catch (error) {
      console.error(`Error in background broadcast processing for campaign ${campaign_id}:`, error);

      // Clean up the dummy record on error
      try {
        await this.db.collection('broadcasts').deleteOne({
          internal_user_id: workspace_id,
          broadCastName: sendBroadcastData?.broadcastName,
          status: 'PENDING'
        });
      } catch (cleanupError) {
        console.error(`Failed to cleanup dummy record for campaign ${campaign_id}:`, cleanupError);
      }

      // Still try to save what was processed so far
      if (broadcastDocuments.length > 0) {
        try {
          await this.db.collection('broadcasts').insertMany(broadcastDocuments);
          console.log(`Partial broadcast data saved for campaign ${campaign_id}`);
        } catch (saveError) {
          console.error(`Failed to save partial broadcast data for campaign ${campaign_id}:`, saveError);
        }
      }

      throw error;
    }
  }

  const sendMessageTemplate = async (waba_id, toPhoneNumber, template, phone_number_id) => {
    try {
      // console.log("--------------------------------");
      // console.log(waba_id);
      // console.log(toPhoneNumber);
      // console.log(template);
      // console.log(phone_number_id);
      // console.log("--------------------------------");

      let templateDetails = await this.BroadcastTemplateMediaRepository.findOne({ where: { template_name: template.name } });

      let data = {
        messaging_product: "whatsapp",
        recipient_type: "individual",
        to: toPhoneNumber,
        type: "template",
        template: {
          name: template.name,
          language: {
            code: template.language
          }
        }
      };

      if (templateDetails) {
        data.template.components = [
          {
            type: "header",
            parameters: [
              {
                type: templateDetails.file_type,
                [templateDetails.file_type]: {
                  link: templateDetails.file_url
                },
              }
            ]
          }
        ];
      }

      // console.log("--------------------------------");
      // console.log(JSON.stringify(data));
      // console.log("--------------------------------");

      const url = `https://amped-express.interakt.ai/api/v17.0/${phone_number_id}/messages`;

      let response = await axios.post(url, data, {
        headers: {
          "x-access-token": process.env.INTERAKT_KEY,
          "x-waba-id": waba_id,
          "Content-Type": "application/json",
        },
      });

      // console.log("--------------------------------");
      // console.log(JSON.stringify(response.data));
      // console.log("--------------------------------");

      return response.data;

    } catch (error) {
      console.error(`Error sending template to ${toPhoneNumber}:`, JSON.stringify(error.response?.data || error.message));
      return {
        error: error?.response?.data || { message: error.message },
      };
    }
  }

  // Export the functions
  module.exports = {
    createBulkBroadcastv2,
    processBroadcastInBackground,
    sendMessageTemplate
  };