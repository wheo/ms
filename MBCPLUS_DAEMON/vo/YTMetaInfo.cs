using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON
{
    public class YTMetaInfo
    {
        public String videoid { get; set; }
        public String title { get; set; }
        public String description { get; set; }        
        public String tag { get; set; }
        public String category { get; set; }
        public String channel_id { get; set; }
        public String privacy { get; set; }
        public String spoken_language { get; set; }
        public String target_language { get; set; }
        public String org_lang_title { get; set; }
        public String org_lang_desc { get; set; }
        public String trans_lang_title { get; set; }
        public String trans_lang_desc { get; set; }
        public String session_id { get; set; }        
        public String thumbnailPath { get; set; }
        public String captionPath { get; set; }
        public String playlist_id { get; set; }        
        public String old_playlist_id { get; set; }
        public String start_time { get; set; }
        public DateTime start_time_DateTime { get; set; }
        public String infomation { get; set; }
    }
}
