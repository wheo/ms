using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON
{
    class LogMgr
    {
        public LogMgr()
        {
            //Hello
        }
        public void Log(String log)
        {
            frmMain.WriteLogThread(log);
        }
    }
}
