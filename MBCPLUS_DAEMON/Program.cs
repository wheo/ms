using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace MBCPLUS_DAEMON
{
    static class Program
    {
        /// <summary>
        /// 해당 응용 프로그램의 주 진입점입니다.
        /// </summary>
        [STAThread]
        static void Main()
        {
            bool no_instance;
            Mutex mtx = new Mutex(true, "BlockDuplicatedExecution", out no_instance);
            IniFile myIni = new IniFile("properties.ini");
            String[] strCallbackURL = new String[2];
            String strServer = myIni.Read("server");
            String strDBinfo = myIni.Read("database");
            String strUid = myIni.Read("uid");
            String strPwd = myIni.Read("pwd");
            String strCharset = myIni.Read("charset");
            String strConn = String.Format("Server={0};Database={1};Uid={2};Pwd={3};Charset={4}", strServer, strDBinfo, strUid, strPwd, strCharset);
            String strThreadCount = myIni.Read("ftp_thread");
            String strEPGUrl = myIni.Read("epgurl");
            String strCallbackURL_m = myIni.Read("callback_url_m");
            String strCallbackURL_b = myIni.Read("callback_url_b");

            strCallbackURL[0] = strCallbackURL_m;
            strCallbackURL[1] = strCallbackURL_b;
            //int threadCount = Int32.Parse(strThreadCount);
            //int threadCount = Convert.ToInt16(strThreadCount);

            Singleton.getInstance().SetStrConn(strConn);
            Singleton.getInstance().SetStrCallbackURL(strCallbackURL);
            Singleton.getInstance().EPG_URL = strEPGUrl;
            //Singleton.getInstance().SetConnection(new MySqlConnection(strConn));

            CdnInfo cdninfo = new CdnInfo();

            cdninfo.strCDNHost = "https://openapi.cloudn.co.kr";
            cdninfo.strCDNMethods = "/cdnservice/downloadpath,/cdnservice/streampath";            
            cdninfo.strCDNMethod = cdninfo.strCDNMethods.Split(',');
            cdninfo.strAPIKey = "2690b505-2ea5-486b-b8d5-ce749277fef0";
            cdninfo.strFTPid = "mbcplus_mbcplus-dn,mbcplus_mbcpvod";
            cdninfo.apiUserid = "mbcplus";
            cdninfo.apiPasswd = "2690b505-2ea5-486b-b8d5-ce749277fef0";
            cdninfo.apiAction = "purge";
            cdninfo.apiDomain = "https://openapi.cloudn.co.kr/cdnservice/purge/PurgeExecutionGT";

            Singleton.getInstance().SetCdnInfo(cdninfo);
            
            if (no_instance)
            {
                mtx.ReleaseMutex();

                YoutubeService youtubeService = new YoutubeService();
                DailmotionService dailymotionService = new DailmotionService();
                ProgramseqService programseqService = new ProgramseqService();
                ClipService clipService = new ClipService();
                ArchiveProgramService archiveProgramService = new ArchiveProgramService();
                ArchiveProgramSeqService archiveProgramSeqService = new ArchiveProgramSeqService();
                ArchiveClipService archiveService = new ArchiveClipService();
                CopyProgramService copyProgramService = new CopyProgramService();
                CopyProgramSeqService copyProgramSeqService = new CopyProgramSeqService();
                CopyClipService copyClipService = new CopyClipService();
                FTPService ftpService = new FTPService();                
                CDNService cdnService = new CDNService();                
                service.BroadPlanService broadPlanService = new service.BroadPlanService();

                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new frmMain());

                broadPlanService.RequestStop();                
                cdnService.RequestStop();                
                ftpService.RequestStop();
                copyClipService.RequestStop();
                copyProgramSeqService.RequestStop();
                copyProgramService.RequestStop();                 
                archiveService.RequestStop();
                archiveProgramSeqService.RequestStop();
                archiveProgramService.RequestStop();
                clipService.RequestStop();
                programseqService.RequestStop();
                dailymotionService.RequestStop();
                youtubeService.RequestStop();
            }
            else
            {
                MessageBox.Show("프로세스 중복 실행이 감지 되었습니다. 프로그램을 종료 합니다.");
            }
            
            //Singleton.getInstance().ConnectionClose();
        }
    }
}
