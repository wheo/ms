using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class ProgramInfo
    {
        public String pid { get; set; }
        public String targetpath { get; set; }
        public String img { get; set; }
        public String org_img { get; set; }
        public String posterimg { get; set; }
        public String org_posterimg { get; set; }                
        public String thumbimg { get; set; }
        public String org_thumbimg { get; set; }
        public String circleimg { get; set; }
        public String org_circleimg { get; set; }
        public String highresimg { get; set; }
        public String org_highresimg { get; set; }
        public String logoimg { get; set; }
        public string org_logoimg { get; set; }
        public int edit_img_count { get; set; }
        public int edit_img_poster_count { get; set; }
        public int edit_img_thumb_count { get; set; }
        public int edit_img_circle_count { get; set; }
        public int edit_img_highres_count { get; set; }
        public int edit_img_logo_count { get; set; }
    }
}
