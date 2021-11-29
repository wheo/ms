using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Net;
using System.Net.Http;
using System.Configuration;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Collections.Specialized;
using Newtonsoft.Json.Linq;


namespace MBCPLUS_DAEMON
{
    public static class Http
    {
        public static byte[] Post(string uri, NameValueCollection pairs)
        {
            byte[] response = null;
            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Content-Type", "application/json");
                response = client.UploadValues(uri, pairs);                
            }
            return response;
        }

        public static byte[] Post(string uri, String strJson)
        {
            byte[] response = null;
            var bytes = Encoding.UTF8.GetBytes(strJson);
            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Content-Type", "application/json");
                response = client.UploadData(uri, "POST", bytes);
            }
            return response;
        }

        public static string PostBody(string uri, string jsonBody)
        {
            // Here we create the request and write the POST data to it.
            var request = (HttpWebRequest)HttpWebRequest.Create(uri);
            request.ContentType = "application/json";

            request.Method = "POST";
            request.Timeout = 1000;

            try
            {
                using (var writer = new StreamWriter(request.GetRequestStream()))
                {
                    writer.Write(jsonBody);
                }

                string response = string.Empty;
                using (WebResponse res = request.GetResponse())
                {
                    Stream respStream = res.GetResponseStream();
                    using (StreamReader sr = new StreamReader(respStream))
                    {
                        response = sr.ReadToEnd();
                    }
                }

                return response;
            }
            catch (WebException wex)
            {
                return null;
            }
        }

        public static String Get(string url)
        {
            int timeout = (int)20000;
            //byte[] responseByte = null;
            String responseString = null;

            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.Timeout = timeout;
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream dataStream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(dataStream, Encoding.UTF8))
                        {
                            responseString = reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception)
            {

            }
            return responseString;
        }
    }
}
