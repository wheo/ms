using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    class CallbackInfo
    {
        public String pk { get; set; }
        public String tc_pk { get; set; }
        public String program_seq_pk { get; set; }
        public String clip_pk { get; set; }
        public String profile_id { get; set; }
        public String transcode_YN { get; set; }        
        public String ftppath { get; set; }
        public String pathfilename { get; set; }
        public String encid { get; set; }
        public String encset { get; set; }
        public String pid { get; set; }
        public String gid { get; set; }
        public String cid { get; set; }        
    }
}
