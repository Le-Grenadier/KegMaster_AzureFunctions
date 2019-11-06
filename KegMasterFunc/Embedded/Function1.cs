using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
//using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.Devices;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;

using System;
//using System.Collections.Generic;
//using System.Threading;
using System.Data.SqlClient;
using System.Text;
using System.Net.Http;
//using System.Diagnostics;

//using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using Newtonsoft.Json;
using Microsoft.Azure.NotificationHubs;
//using System.Data;
//using System.Net;
//using System.Net.Http.Formatting;
//using Microsoft.Rest;

namespace KegMasterFunc
{
    public static class Function1
    {
        private static HttpClient client = new HttpClient();
        private static readonly string[] kegItemFields =
        {
            "Alerts", 
            "TapNo",
            "Name",
            "Style",
            "Description",
            "DateKegged",
            "DateAvail",
            "PourEn",
            "PourNotification",
            "PourQtyGlass",
            "PourQtySample",
            "PressureCrnt",
            "PressureDsrd",
            "PressureDwellTime",
            "PressureEn",
            "QtyAvailable",
            "QtyReserve",
            "Version",
            "CreatedAt",
            "UpdatedAt",
            "Deleted"
        };

        [FunctionName("Function1")]
        [return: Blob("output-container/{id}")]
        public static async Task Run([IoTHubTrigger("messages/events", Connection = "iothub_endpoint")]EventData message, ILogger log)
        {
            var str = Environment.GetEnvironmentVariable("sqldb_connection");

            log.LogInformation($"C# IoT Hub trigger function processed a message: {Encoding.UTF8.GetString(message.Body.Array)}");

            var raw_obj = JObject.Parse(Encoding.UTF8.GetString(message.Body.Array)).Root;

            /*-----------------------------------------------------------------
             If 'Id' present - a row is being updated
            -----------------------------------------------------------------*/
            var id_field = (string)raw_obj["Id"];
            if (id_field != null)
            {
                await updateRow(str, raw_obj, log);
            } else
            {
                log.LogError($"Operation Failed to specify 'Id' field");
            }

            /*-----------------------------------------------------------------
             If 'RqId' present - a row is being requested
            -----------------------------------------------------------------*/
            var req_id = (string)raw_obj["ReqId"];
            if (req_id != null)
            {
                await getRow(str, raw_obj, log);
            }
            else
            {
                log.LogError($"Operation Failed to specify 'ReqId'");
            }


            /*-----------------------------------------------------------------
             If 'Alerts' present - a send push notification
            -----------------------------------------------------------------*/
            var alerts = (string)raw_obj["Alerts"];
            if (alerts != null)
            {
                await doPushNotify(str, raw_obj, log);
            }
            else
            {
                log.LogError($"Operation Failed to specify 'ReqId'");
            }
        }

        public static async Task getRow(string conStr, JToken json, ILogger log)
        {
            using (SqlConnection conn = new SqlConnection(conStr))
            {
                string id = "";
                string tap = "";
                string ret = "";
                string qrySelect = "";
                var devStr = Environment.GetEnvironmentVariable("device_endpoint");
                var serviceClient = ServiceClient.CreateFromConnectionString(devStr);
                /* 
                 * For now the request Id will be ignored, later this field will
                 * be used to identify and return data only for the provided 
                 * Keezer Id. Currently this isn't important since only one 
                 * keezer is active. 
                 * 
                 * I feel like there's a better way to do this, but time is not
                 * in favor of finding it atm. 
                 */
                id = (string)json["ReqId"].Value<string>();
                id = Regex.Replace(id, "\"", "");
                log.LogInformation($"ReqId: {id}");

                tap = (string)json["TapNo"].Value<string>();
                tap = tap == null ? "" : Regex.Replace(tap, "\"", ""); ;
                log.LogInformation($"TapNo: {tap}");

                qrySelect = (string)json["qrySelect"].Value<string>();
                qrySelect = qrySelect == null ? "" : Regex.Replace(qrySelect, "\"", "");
                log.LogInformation($"qrySelect: {qrySelect}");

                /* Update provided row if at least one Key-value has been provided */
                if ( (tap.Length > 0 && tap != "")
                  && (qrySelect.Length > 0 && qrySelect != "") )
                {
                    string qs = qrySelect.Contains("*") && !qrySelect.Contains("TapNo") ? qrySelect : qrySelect + ", TapNo";
                    string query = $"SELECT {qs} FROM KegItems WHERE TapNo='{tap}' ORDER BY UpdatedAt DESC";
                    log.LogInformation($"Query: {query}");

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        SqlDataReader row = await cmd.ExecuteReaderAsync();
                        
                        log.LogInformation($"Queried Rows Has Rows?: {row.HasRows}");
                        IEnumerable< Dictionary<string, object>> e = Serialize(row);
                        ret = JsonConvert.SerializeObject(e, Formatting.Indented);
                    }
                }


                var commandMessage = new Message(Encoding.ASCII.GetBytes(ret));
                /*
                 * The device Id should be determined dynamically in the future.
                 */
                await serviceClient.SendAsync((string)"KegMaster", commandMessage);
            }
        }
        public static async Task updateRow(string conStr, JToken json, ILogger log)
        {
            using (SqlConnection conn = new SqlConnection(conStr))
            {
                string id = "";
                string vals = "";
                string comma = "";

                id = (string)json["Id"].Value<string>();
                id = Regex.Replace(id, "\"", "");
                log.LogInformation($"Row Id: {id}");

                /* Build list of Key Value pairs to update */
                foreach (var e in kegItemFields)
                {
                    JToken j = json[e];
                    if (null != j)
                    {
                        string v = j.Value<string>();
                        v = Regex.Replace(v, "\"", "");

                        string val = $"[{e}] = '{v}' ";

                        vals = vals + comma + val;
                        comma = ",";
                    }
                }
                log.LogInformation($"update: {vals}");

                /* Update provided row if at least one Key-value has been provided */
                if (vals.Length > 0 && null != id)
                {
                    string query = $"UPDATE KegItems SET {vals} WHERE [Id]='{id}'";
                    log.LogInformation($"Query: {query}");

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        var rowelems = await cmd.ExecuteNonQueryAsync();
                        log.LogInformation($"{rowelems} row-elements were updated");
                    }
                }
            }
        }
        public static async Task doPushNotify(string conStr, JToken json, ILogger log)
        {
            log.LogInformation($"Processing Push Notification");

            /*---------------------------------------------
            Get connection to notification hub
            ---------------------------------------------*/
            var NotHubConStr = Environment.GetEnvironmentVariable("KegMaster_NotificationHubEndpoint");
            NotificationHubClient hub = NotificationHubClient.CreateClientFromConnectionString(NotHubConStr, "KegMaster_NotificationHub");
  
            /*---------------------------------------------
            Alert Text - Check validity again just for fun 
            ---------------------------------------------*/
            string alertText = json["Alerts"].ToString();
            if(alertText == null) { return; }
            log.LogInformation($"\tAlert Text: {alertText}");

            /*---------------------------------------------
            Determine the 'BadgeValue'
             - This will determine the number in the 
               'red balloon' above the iOS app icon.
            ---------------------------------------------*/
            string badgeValue = "1"; // TODO: Make this dynamic

            /*---------------------------------------------
            Send iOS alert
            ---------------------------------------------*/
            var iOSalert =
            "{\"aps\":{\"alert\":\"" + alertText + "\", \"badge\":" + badgeValue + ", \"sound\":\"default\"},"
            + "\"inAppMessage\":\"" + alertText + "\"}";
            NotificationOutcome ret = await hub.SendAppleNativeNotificationAsync(iOSalert);
            log.LogInformation($"\tApple Notification Outcome: {ret.ToString()}");

            /*---------------------------------------------
            Send Android alert 
             - TODO: Impliment
            ---------------------------------------------*/
            /*
            #error Add support for Android later
            var androidAlert = "{\"data\":{\"message\": \"" + alertText + "\"}}";
             NotificationOutcome ret = await hub.SendGcmNativeNotificationAsync(androidAlert);
            log.LogInformation($"\tApple Notification Outcome: {ret.ToString()}");
            */
        }

        public static IEnumerable<Dictionary<string, object>> Serialize(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
                results.Add(SerializeRow(cols, reader));

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
                result.Add(col, reader[col]);
            return result;
        }
    }
}

