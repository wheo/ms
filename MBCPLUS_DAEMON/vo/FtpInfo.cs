using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class FtpInfo
    {
        public String pk { get; set; }
        public String clip_pk { get; set; }
        public String customer_id { get; set; }
        public String srcpath { get; set; }
        public String targetpath { get; set; }
        public String targetfilename { get; set; }
        public String old_targetfilename { get; set; }
        public String attribute { get; set; }
        public String customer_name { get; set; }
        public String transcoding_YN { get; set; }
        public String alias_YN { get; set; }
        public String s_title { get; set; }
        public String title { get; set; }
        public String broaddate { get; set; }
        public String sportskind { get; set; }
        public String team1 { get; set; }
        public String team2 { get; set; }
        public String inning { get; set; }
        public String host { get; set; }
        public String port { get; set; }
        public String path { get; set; }
        public String id { get; set; }
        public String pw { get; set; }
        public String status { get; set; }
        public String type { get; set; }
        public String clip_YN { get; set; }
        public String pid { get; set; }
        public String gid { get; set; }
        public String cid { get; set; }
        public String program_img_type { get; set; }
        public String smr_pid { get; set; }
        public String smr_img_type { get; set; }
        public String metahub_YN { get; set; }
        public String s_metahub_YN { get; set; }
        public String cdn_img { get; set; }
        public String cdn_mov { get; set; }
        public String s_ip4addr { get; set; }
        public int clip_img_edit_count { get; set; }
        public int clip_mov_edit_count { get; set; }
        public int program_img_edit_count { get; set; }
        public int program_posterimg_edit_count { get; set; }
        public int program_thumbimg_edit_count { get; set; }
        public int program_circleimg_edit_count { get; set; }
        public int smr_program_img_edit_count { get; set; }
        public int smr_program_posterimg1_edit_count { get; set; }
        public int smr_program_posterimg2_edit_count { get; set; }
        public int smr_program_bannerimg_edit_count { get; set; }
        public int smr_program_thumbimg_edit_count { get; set; }
        public int program_seq_img_edit_count { get; set; }
        public int youtube_img_edit_count { get; set; }
        public int dailymotion_img_edit_count { get; set; }
    }
}
