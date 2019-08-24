using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Data.SqlClient;
using System.Text;
using System.Net.Http;
using System.Diagnostics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Data;

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
        public static async Task Run([IoTHubTrigger("messages/events", Connection = "iothub_endpoint")]EventData message, ILogger log)
        {
            string id = "";
            var str = Environment.GetEnvironmentVariable("sqldb_connection");

            log.LogInformation($"C# IoT Hub trigger function processed a message: {Encoding.UTF8.GetString(message.Body.Array)}");

            var raw_obj = JObject.Parse(Encoding.UTF8.GetString(message.Body.Array)).Root;
           
            /* Id is a special case, as it is always required */
            id = (string)raw_obj["Id"].Value<string>();
            id = Regex.Replace( id, "\"", "");

            log.LogInformation($"data: {id}");

            using (SqlConnection conn = new SqlConnection(str))
            {
                string vals = "";
                string comma = "";

                /* Build list of Key Value pairs to update */
                foreach ( var e in kegItemFields )
                {
                    JToken j = raw_obj[e];
                    if (null != j)
                    {
                        string v = j.Value<string>();
                        string val = $"[{e}] = {v} ";

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
    }
}
