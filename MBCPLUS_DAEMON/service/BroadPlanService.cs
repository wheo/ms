using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using System.IO;
using System.Xml;
using MySql.Data;
using MySql.Data.MySqlClient;
using Newtonsoft.Json.Linq;

namespace MBCPLUS_DAEMON.service
{
    class BroadPlanService
    {
        private Boolean _shouldStop = false;
        //private ConnectionPool connPool;
        private Log log;
        private SqlMapper mapper;

        public BroadPlanService()
        {
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

        void MakeAlltheKPOP()
        {
            vo.EPGInfo alltheKpop = new vo.EPGInfo();

            String date;
            for (int i = -1; i < 6; i++)
            {
                date = Util.GetCurrentDate(i);

                alltheKpop.SID = String.Format("{0}-1", date);
                alltheKpop.MID = "ALLTHEKPOP";
                alltheKpop.ch_name = "ALLTHEKPOP";
                alltheKpop.ch_no = "3";
                alltheKpop.ProgramName = "DAILY K-POP";
                alltheKpop.Grade = "0";
                alltheKpop.StartYMD = date;
                alltheKpop.StartTime = "05:00:00";
                alltheKpop.EndTime = "07:00:00";
                mapper.InsertEpginfo(alltheKpop);

                alltheKpop.SID = String.Format("{0}-2", date);
                alltheKpop.ProgramName = "Weekley Idol speacial";
                alltheKpop.StartTime = "07:00:00";
                alltheKpop.EndTime = "13:00:00";
                mapper.InsertEpginfo(alltheKpop);

                alltheKpop.SID = String.Format("{0}-3", date);
                alltheKpop.ProgramName = "DALY K-POP";
                alltheKpop.StartTime = "13:00:00";
                alltheKpop.EndTime = "19:00:00";
                mapper.InsertEpginfo(alltheKpop);

                alltheKpop.SID = String.Format("{0}-4", date);
                alltheKpop.ProgramName = "I AM IDOL";
                alltheKpop.StartTime = "19:00:00";
                alltheKpop.EndTime = "23:00:00";
                mapper.InsertEpginfo(alltheKpop);

                alltheKpop.SID = String.Format("{0}-5", date);
                alltheKpop.ProgramName = "Weekley Idol speacial";
                alltheKpop.StartTime = "23:00:00";
                alltheKpop.EndTime = "05:00:00";
                mapper.InsertEpginfo(alltheKpop);
            }
        }

        void Run()
        {
            Thread.Sleep(10000);
            //frmMain.WriteLogThread("BroadPlan Service Start...");
            log.logging("Service Start...");
            String EPG_URL = Singleton.getInstance().EPG_URL;
            String response;
            String today;
            String[] channel = new String[3];
            int addDay = -1;
            channel[0] = "1";
            channel[1] = "2";
            channel[2] = "4";
            int ch_index = 0;

            //MakeAlltheKPOP();

            while (!_shouldStop)
            {
                try
                {                    
                    //channel 1 : 드라마, 2 : every1 4: Music
                    Thread.Sleep(1000);

                    today = Util.GetCurrentDate(addDay);
                    
                    response = "";                    
                    response = Http.Get(String.Format("{0}?nday={1}&code={2}", EPG_URL, today, channel[ch_index]));
                    if ( String.IsNullOrEmpty(response ))
                    {
                        continue;
                    }
                    addDay++;

                    //log.logging(String.Format("{0}?nday={1}&code={2}", EPG_URL, today, channel[ch_index]));

                    // response 를 받았을 때 기존 스케줄 삭제
                    mapper.DeleteEpgInfo(today, channel[ch_index]);
                    //log.logging(String.Format("Delete EpgInfo : day {0} ch_no : {1}", today, channel[ch_index]));

                    XmlDocument xmldoc = new XmlDocument();
                    XmlNodeList xmlnodes;
                    xmldoc.LoadXml(response);
                    XmlElement root = xmldoc.DocumentElement;

                    xmlnodes = root.ChildNodes;

                    vo.EPGInfo epgInfo = new vo.EPGInfo();

                    foreach (XmlNode eventnode in xmlnodes)
                    {
                        foreach (XmlNode node in eventnode)
                        {
                            switch (node.Name)
                            {
                                case "SID":
                                    epgInfo.SID = node.InnerText.Trim();
                                    break;
                                case "MID":
                                    epgInfo.MID = node.InnerText.Trim();
                                    break;
                                case "ch_no":
                                    epgInfo.ch_no = node.InnerText.Trim();
                                    break;
                                case "ch_name":
                                    epgInfo.ch_name = node.InnerText.Trim();
                                    break;
                                case "ProgramName":
                                    epgInfo.ProgramName = node.InnerText.Trim();
                                    break;
                                case "ProgramSubName":
                                    epgInfo.ProgramSubName = node.InnerText.Trim();
                                    break;
                                case "StartYMD":
                                    epgInfo.StartYMD = node.InnerText.Trim();
                                    break;
                                case "StartTime":
                                    epgInfo.StartTime = node.InnerText.Trim();
                                    break;
                                case "EndTime":
                                    epgInfo.EndTime = node.InnerText.Trim();
                                    break;
                                case "Frequency":
                                    epgInfo.Frequency = node.InnerText.Trim();
                                    break;
                                case "HD":
                                    epgInfo.HD = node.InnerText.Trim();
                                    break;
                                case "Duration":
                                    epgInfo.Duration = node.InnerText.Trim();
                                    break;
                                case "Grade":
                                    epgInfo.Grade = node.InnerText.Trim();
                                    break;
                                case "Suwha":
                                    epgInfo.Suwha = node.InnerText.Trim();
                                    break;
                            }
                        }
                        //  insert 부분
                        mapper.InsertEpginfo(epgInfo);
                    }
                    if (addDay == 6)
                    {
                        addDay = -1;
                        ch_index++;
                        if (ch_index == 3)
                        {
                            ch_index = 0;
                            log.logging("MakeAlltheKpop");
                            // Make AllthekPOP
                            MakeAlltheKPOP();
                            for (int i = 0; i < 60 * 60; i++) // 5분 마다 갱신
                            {                                
                                Thread.Sleep(1000);
                                if (_shouldStop)
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    log.logging(e.ToString());
                    today = Util.GetCurrentDate(addDay);
                }
            }
        }
    }
}