using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class SmrProgramInfo
    {
        public String pid { get; set; }
        public String targetpath { get; set; }
        public String img { get; set; }
        public String org_img { get; set; }
        public String posterimg1 { get; set; }
        public String org_posterimg1 { get; set; }
        public String posterimg2 { get; set; }
        public String org_posterimg2 { get; set; }
        public String bannerimg { get; set; }
        public String org_bannerimg { get; set; }
        public String thumbimg { get; set; }        
        public String org_thumbimg { get; set; }
        public String coverimg { get; set; }
        public String org_coverimg { get; set; }
        public int edit_img_count { get; set; }
        public int edit_img_poster1_count { get; set; }
        public int edit_img_poster2_count { get; set; }
        public int edit_img_banner_count { get; set; }
        public int edit_img_thumb_count { get; set; }
        public int edit_img_cover_count { get; set; }
    }
}
