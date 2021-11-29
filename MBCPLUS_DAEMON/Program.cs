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
            String strBBMCHost = myIni.Read("bbmchost");

            string strSMCyoutubueAPI = myIni.Read("smc_youtube");
            Singleton.getInstance().SMCyoutubueAPI = strSMCyoutubueAPI;

            //http://smc-api-production.azurewebsites.net/api/youtubeinfo post

            strCallbackURL[0] = strCallbackURL_m;
            strCallbackURL[1] = strCallbackURL_b;
            //int threadCount = Int32.Parse(strThreadCount);
            //int threadCount = Convert.ToInt16(strThreadCount);

            //test 프로그램 여부
            Singleton.getInstance().setTestMode(myIni.Read("test")); // ini를 읽어서 true면 archive 및 cdn test 경로로 생성됨

            Singleton.getInstance().BBMChost = strBBMCHost;

            Singleton.getInstance().SetStrConn(strConn);
            Singleton.getInstance().SetStrCallbackURL(strCallbackURL);
            Singleton.getInstance().EPG_URL = strEPGUrl;

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
#if true
                YoutubeService youtubeService = null;
                DailmotionService dailymotionService = null;
                
                if (!Singleton.getInstance().Test)                
                {
                    youtubeService = new YoutubeService();
                    dailymotionService = new DailmotionService();
                }
                
                //ClipService clipService = new ClipService(); /*삭제 예정 MetaHub 관련*/
                ProgramSeqService programSeqService = new ProgramSeqService();
                ArchiveProgramService archiveProgramService = new ArchiveProgramService();
                ArchiveSmrProgramService archiveSmrProgramService = new ArchiveSmrProgramService();
                ArchiveProgramSeqService archiveProgramSeqService = new ArchiveProgramSeqService();
                ArchiveClipService archiveClipService = new ArchiveClipService();
                CopyProgramService copyProgramService = new CopyProgramService();
                CopySmrProgramService copySmrProgramService = new CopySmrProgramService();
                CopyProgramSeqService copyProgramSeqService = new CopyProgramSeqService();
                CopyClipService copyClipService = new CopyClipService();
                FTPService ftpService = new FTPService();
                CDNService cdnService = new CDNService();
                service.BroadPlanService broadPlanService = new service.BroadPlanService();
#endif
                service.SmrYoutubeAPI smrYoutubeAPI = new service.SmrYoutubeAPI();

                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new frmMain());

                smrYoutubeAPI.RequestStop();
#if true
                broadPlanService.RequestStop();                
                cdnService.RequestStop();                
                ftpService.RequestStop();
                copyClipService.RequestStop();
                copyProgramSeqService.RequestStop();
                copySmrProgramService.RequestStop();
                copyProgramService.RequestStop();                 
                archiveClipService.RequestStop();
                archiveProgramSeqService.RequestStop();
                archiveSmrProgramService.RequestStop();
                archiveProgramService.RequestStop();
                programSeqService.RequestStop();
                //clipService.RequestStop();
                
                if (!Singleton.getInstance().Test)
                {
                    dailymotionService.RequestStop();
                    youtubeService.RequestStop();
                }
#endif
            }
            else
            {
                MessageBox.Show("프로세스 중복 실행이 감지 되었습니다. 프로그램을 종료 합니다.");
            }
            
            //Singleton.getInstance().ConnectionClose();
        }
    }
}
