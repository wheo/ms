using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON
{
    class ClipInfo
    {
        public String imgsrcpath { get; set; }
        public String clipsrcpath { get; set; }
        public String cdnimg { get; set; }
        public String cdnmov { get; set; }
        //public String m_dstpath;
        public String orgimgname { get; set; }
        public String orgclipname { get; set; }
        public String clip_pk { get; set; }
        public String metahub_YN { get; set; }
        public String gid { get; set; }
        public String cid { get; set; }
        public String archive_date { get; set; }
        public String section { get; set; }
        public String idolclip_YN { get; set; }
        public String idolvod_YN { get; set; }
        public String idolvote_YN { get; set; }
        public String yt_isuse { get; set; }
        public String yt_videoid { get; set; }
        public String dm_isuse { get; set; }
        public String dm_videoid { get; set; }        
        public String archive_img { get; set; }
        public String archive_clip { get; set; }
    }
}
