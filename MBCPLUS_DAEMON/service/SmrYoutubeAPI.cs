using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.service
{

    class Request
    {
        private string _clipid;
        public string clipid
        {
            get
            {
                return _clipid;
            }
            set
            {
                if (!string.IsNullOrEmpty(value))
                {
                    _clipid = "M12_" + value;
                }
                else
                {
                    _clipid = null;
                }
            }
        }
    }

    class SmrApiObject
    {
        public string clipid { get; set; }        
        public string youtubevideoid { get; set; }
        public string channelid { get; set; }
        public string playlistid { get; set; }
        public string assetid { get; set; }
        public string senddate { get; set; }
        public string clipmodifydate { get; set; }
        public string sendstarttime { get; set; }
        public string sendendtime { get; set; }
    }

    class SmrYoutubeAPI
    {
        private bool _signal = false;
        private Log logger;

        public void RequestStop()
        {
            _signal = true;
        }

        public SmrYoutubeAPI()
        {
            logger = new Log(this.GetType().Name);
            DoWork();            
        }

        public List<Request> GetRequestList()
        {
            DataTable dt = new DataTable();
            string query = String.Format(@"
SELECT Y.cid FROM TB_YOUTUBE Y WHERE Y.type = 'SMR' AND Y.videoid IS NULL
AND Y.channel_id != ''
AND Y.edit_time > DATE_ADD(NOW(), INTERVAL - 3 DAY)");
            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
            {
                conn.Open();
                MySqlDataAdapter adpt = new MySqlDataAdapter(query, conn);
                adpt.Fill(dt);
            }
            return dt.AsEnumerable().Select(row => new Request
            {
                clipid = row.Field<string>("cid")
            }).ToList();
        }

        public int UpdateYoutubeInfo(SmrApiObject s)
        {
            int ret = 0;
            string query = "UPDATE TB_YOUTUBE set videoid = @videoid, assetid = @assetid, senddate = @senddate, clipmodifydate = @clipmodifydate, sendstarttime = @sendstarttime, sendendtime = @sendendtime WHERE cid = @id";
            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn())) 
            {
                conn.Open();
                MySqlCommand cmd = new MySqlCommand(query, conn);
                cmd.Parameters.AddWithValue("@id", s.clipid.Replace("M12_", ""));
                cmd.Parameters.AddWithValue("@assetid", s.assetid);
                cmd.Parameters.AddWithValue("@videoid", s.youtubevideoid);
                cmd.Parameters.AddWithValue("@senddate", s.senddate);
                cmd.Parameters.AddWithValue("@clipmodifydate", s.clipmodifydate);
                cmd.Parameters.AddWithValue("@sendstarttime", s.sendstarttime);
                cmd.Parameters.AddWithValue("@sendendtime", s.sendendtime);
                cmd.Prepare();
                ret = cmd.ExecuteNonQuery();
            }
            return ret;
        }

        private void DoWork()
        {
            logger.logging("Service Start...");
            TimeSpan ts_interval = new TimeSpan(1, 0, 0);
            TimeSpan ts = new TimeSpan(0, 0, 10);
            var task = Task.Run(async () =>
            {
                while (!_signal)
                {
                        List<Request> req = GetRequestList();
                        req.ForEach(r =>
                        {
                            try
                            {
                                string jsonBody = JsonConvert.SerializeObject(r);
                                logger.logging(jsonBody);
                                List<SmrApiObject> response = JsonConvert.DeserializeObject<List<SmrApiObject>>(
                                    Http.PostBody(Singleton.getInstance().SMCyoutubueAPI, jsonBody)
                                    );
                                if (response.Count > 0)
                                {
                                    SmrApiObject s = response[0];
                                    UpdateYoutubeInfo(s);
                                    logger.logging(string.Format($"({s.clipid}) smr youtube videoid({s.youtubevideoid}) responsed"));
                                }
                            }
                            catch (Exception ex)
                            {
                                logger.logging(ex.ToString());
                            }                            
                            Thread.Sleep(ts);
                        });
                    
                    await Task.Delay(ts_interval);
                }
            });
            
            logger.logging(string.Format($"{this.GetType().Name} task running ..."));
        }
    }
}
