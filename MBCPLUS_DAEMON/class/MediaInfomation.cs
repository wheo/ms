using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON
{
    class MediaInfomation
    {        
        public String duration { get; set; }
        public String filesize { get; set; }
        public String v_streamkimd { get; set; }
        public String v_format { get; set; }
        public String v_profile { get; set; }
        public String v_version { get; set; }
        public String v_gop { get; set; }
        public String v_cabac {get;set;}
        public String v_codec { get; set; }
        public String v_bitrate { get; set; }
        public String a_codec { get; set; }
        public String a_bitrate { get; set; }
        public String v_resolution_x { get; set; }
        public String v_resolution_y { get; set; }        
    }
}