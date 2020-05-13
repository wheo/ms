using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class ProgramSeqInfo
    {
        public String pk { get; set; }
        public String gid { get; set; }
        public String imgsrcpath { get; set; }
        public String orgimgname { get; set; }
        public String cdn_img { get; set; }
        public String archive_date { get; set; }
        public String section { get; set; }
        public int edit_img_count {get;set;}
    }
}
