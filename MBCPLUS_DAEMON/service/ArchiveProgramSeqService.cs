using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using System.IO;
using MySql.Data;
using MySql.Data.MySqlClient;
using MediaInfoLib;

namespace MBCPLUS_DAEMON
{
    class ArchiveProgramSeqService
    {        
        private Boolean _shouldStop = false;        
        
        //private String m_clipsrcpath;
        //private String m_dstpath;        

        //private String m_sql = "";
        //private ConnectionPool connPool;
        private Log log;
        private SqlMapper mapper;

        public ArchiveProgramSeqService()
        {
            // put class name
            mapper = new SqlMapper();
            log = new Log(this.GetType().Name);
            DoWork();
        }

        void DoWork()
        {
            Thread t1 = new Thread(new ThreadStart(Run));
            t1.Start();
        }

        public void RequestStop()
        {
            _shouldStop = true;            
        }

        void Run()
        {               
            String status = null;
            vo.ProgramSeqInfo programSeqInfo = new vo.ProgramSeqInfo();            
            //MySqlCommand cmd;

            //connPool = new ConnectionPool();
            //connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));            
            
            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Archive Program Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    DataSet ds = new DataSet();
                    mapper.GetArchiveProgramSeqServiceInfo(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        try
                        {                            
                            programSeqInfo.pk = r["program_seq_pk"].ToString();
                            programSeqInfo.gid = r["gid"].ToString();
                            
                            programSeqInfo.imgsrcpath = r["imgsrcpath"].ToString();
                            programSeqInfo.orgimgname = r["orgimgname"].ToString();
                            programSeqInfo.src_cue = r["src_cue"].ToString();
                            programSeqInfo.org_cue = r["org_cue"].ToString();
                            programSeqInfo.src_script = r["src_script"].ToString();
                            programSeqInfo.org_script = r["org_script"].ToString();
                            programSeqInfo.gid = r["gid"].ToString();
                            programSeqInfo.cdn_img = r["cdnurl_img"].ToString();
                            programSeqInfo.archive_date = r["archive_date"].ToString();                            
                            programSeqInfo.section = r["section"].ToString();
                            programSeqInfo.edit_img_count = Convert.ToInt32(r["edit_img_count"].ToString());
                            programSeqInfo.edit_cue_count = Convert.ToInt32(r["edit_cue_count"].ToString());
                            programSeqInfo.edit_script_count = Convert.ToInt32(r["edit_script_count"].ToString());

                            status = r["status"].ToString();
                           
                            mapper.UpdateArchiveServiceRunning(programSeqInfo.gid);

                            frmMain.WriteLogThread(String.Format(@"gid({0}) is Archive", programSeqInfo.gid));
                            //String targetPath = "";

                            // 스포츠, 예능 구분해야함(프로그램 정보로부터 가져올 수 있음)
                            StringBuilder sb = new StringBuilder();
                            if (Singleton.getInstance().Test)
                            {
                                sb.Append(Util.getTestPath());
                            } else
                            {
                                sb.Append(Util.getSectionPath(programSeqInfo.section));
                            }
                            sb.Append(programSeqInfo.archive_date);
                            sb.Append(Path.DirectorySeparatorChar);
                            sb.Append(programSeqInfo.gid);

                            programSeqInfo.targetpath = sb.ToString();

                            try
                            {
                                if (!Directory.Exists(programSeqInfo.targetpath))
                                {
                                    Directory.CreateDirectory(programSeqInfo.targetpath);
                                }
                            }
                            catch (Exception e)
                            {
                                frmMain.WriteLogThread(e.ToString());
                            }

                            frmMain.WriteLogThread(programSeqInfo.targetpath);

                            mapper.ArchiveProgramSeq(programSeqInfo);

                            if (String.IsNullOrEmpty(programSeqInfo.imgsrcpath) && programSeqInfo.cdn_img.Length > 7)
                            {
                                mapper.UpdateProgramSeqStatus(programSeqInfo.gid, "Completed");
                                frmMain.WriteLogThread(String.Format(@"[ArchiveProgramService] cdnimg : {0} is already exist, gid = {1}", programSeqInfo.cdn_img, programSeqInfo.gid));
                            }
                            else
                            {
                                mapper.UpdateProgramSeqStatus(programSeqInfo.gid, "Failed", "회차 이미지 없음");
                            }
                        }
                        catch (Exception e)
                        {
                            log.logging(e.ToString());
                            //frmMain.WriteLogThread("[ArchveProgramService] " + e.ToString());
                        }
                    }
                    ds.Clear();
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread(e.ToString());
                    log.logging(e.ToString());
                }                
                Thread.Sleep(1000);
            }
            //connPool.ConnectionDisPose();
            log.logging("Thread Terminate" + _shouldStop);
        }
    }
}