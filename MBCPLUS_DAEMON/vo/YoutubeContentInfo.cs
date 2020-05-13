using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class YoutubeContentInfo
    {
        public String videoid { get; set; }
        public String clip_pk { get; set; }
        public String gid { get; set; }
        public String cid { get; set; }
        //youtube default customer_id is 9
        public String customerId { get; } = "9";
        public String srcImg {get;set;}
        public String srcMovie { get; set; }
        public String srcSubtitle { get; set; }
    }
}
