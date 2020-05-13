using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class DailymotionContentInfo
    {
        public String videoid { get; set; }
        public String cid { get; set; }
        //Dailymotion default customer_id is 10
        public String customerId { get; } = "10";
        public String srcImg { get; set; }
        public String srcMovie { get; set; }
        public String srcSubtitle { get; set; }
    }
}
