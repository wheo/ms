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
        public String subtitlesrcpath { get; set; }
        public String yt_upload_img { get; set; }
        public String yt_upload_srt { get; set; }
        public String cdnimg { get; set; }
        public String cdnmov { get; set; }
        public String cdnsubtitle { get; set; }
        public String yt_cdn_img { get; set; }
        public String yt_cdn_srt { get; set; }
        //public String m_dstpath;
        public String orgimgname { get; set; }
        public String orgclipname { get; set; }
        public String orgsubtitlename { get; set; }
        public String yt_org_img { get; set; }
        public String yt_org_srt { get; set; }
        public String clip_pk { get; set; }
        public String metahub_YN { get; set; }
        public String gid { get; set; }
        public String cid { get; set; }
        //public String archive_date { get; set; }
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
        public String archive_subtitle { get; set; }
        public String yt_ar_img { get; set; }
        public String yt_ar_srt { get; set; }
        public String isuse { get; set; }
        public int edit_img_count { get; set; }
        public int edit_clip_count { get; set; }
        public int edit_vod_img_count { get; set; }
        public int edit_vod_clip_count { get; set; }
        public String yt_type { get; set; }
        public String isvod { get; set; }
    }
}
