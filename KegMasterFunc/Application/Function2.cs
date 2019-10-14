using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KegMasterFunc
{
    public static class Function2
    {
        [FunctionName("Function2")]
        public static async Task Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var str = Environment.GetEnvironmentVariable("sqldb_connection");

            log.LogInformation("C# HTTP post trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            log.LogInformation(requestBody);

            dynamic json = JsonConvert.DeserializeObject(requestBody);

            var req_id = (string)json["ReqId"];
            if (req_id != null)
            {
                string updtStr = string.Format("{{Id:\"{0}\", {1}:\"{2}\"}}", json["Id"], json["qrySelect"], json["qryValue"]);
                log.LogInformation(updtStr);
                JObject updtJson = JObject.Parse(updtStr);
                await Function1.updateRow(str, updtJson, log);

                /* Format = """ReqId:"", TapNo:{n}, qrySelect:{json_key}'""" */
                string notifyStr = string.Format("{{ReqId:\"\", TapNo:{0}, qrySelect:\"{1}\"}}", json["TapNo"], json["qrySelect"]);
                log.LogInformation(notifyStr);
                JObject notifyJson = JObject.Parse(notifyStr);
                await Function1.getRow(str, notifyJson, log);
            }
        }
    }
}
