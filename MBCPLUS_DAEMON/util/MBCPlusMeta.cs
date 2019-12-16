using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON
{
    class MBCPlusMeta
    {
        public MBCPlusMeta()
        {
            log = new Log(this.GetType().Name);            
        }

        private XmlDocument m_xmlDoc = null;
        private String m_strJson = null;
        private Log log = null;

        public void MakeXML(Dictionary<String, Object> map)
        {
            XmlDocument xmlDoc = new XmlDocument();
            try
            {                
                xmlDoc.AppendChild(xmlDoc.CreateXmlDeclaration("1.0", "UTF-8", null));
                XmlNode root = xmlDoc.CreateElement("Information");
                /*
                xmlDoc.DocumentElement.SetAttribute("publisher", "MBCPlus");
                xmlDoc.DocumentElement.SetAttribute("publicationTime", "YYYYMMDD");
                xmlDoc.DocumentElement.SetAttribute("rightsOwner", "MBCPlus");
                xmlDoc.DocumentElement.SetAttribute("version", "1");
                 */

                xmlDoc.AppendChild(root);

                XmlNode sTag, sTagLevel1, sTagLevel2, sTagLevel3, sTagLevel4;
                //XmlCDataSection CData;

                Object strValue = null;

                sTag = xmlDoc.CreateElement("ProgramDescription");
                //sTag.InnerText = map["contentid"];            

                sTagLevel1 = xmlDoc.CreateElement("GroupInformationTable");
                sTag.AppendChild(sTagLevel1);

                sTagLevel2 = xmlDoc.CreateElement("GroupInformation");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel3 = xmlDoc.CreateElement("BasicDescription");
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel4 = xmlDoc.CreateElement("contentid");
                sTagLevel4.InnerText = map["s_contentid"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("cornerid");
                sTagLevel4.InnerText = map["s_cornerid"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);
                
                sTagLevel4 = xmlDoc.CreateElement("broaddate");
                sTagLevel4.InnerText = map["s_broaddate"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);                 

                sTagLevel4 = xmlDoc.CreateElement("contentnumber");
                sTagLevel4.InnerText = map["s_contentnumber"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("cornernumber");
                sTagLevel4.InnerText = map["s_cornernumber"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("preview");
                sTagLevel4.InnerText = map["s_preview"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("title");
                sTagLevel4.InnerText = map["s_title"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("actor");
                sTagLevel4.InnerText = map["s_actor"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("targetage");
                sTagLevel4.InnerText = map["s_targetage"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("genre");
                sTagLevel4.InnerText = map["s_genre"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel1 = xmlDoc.CreateElement("ClipInformationTable");
                sTag.AppendChild(sTagLevel1);

                sTagLevel2 = xmlDoc.CreateElement("ClipInformation");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel3 = xmlDoc.CreateElement("BasicDescription");
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel4 = xmlDoc.CreateElement("clipid");
                sTagLevel4.InnerText = map["cid"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("title");
                sTagLevel4.InnerText = map["title"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("synopsis");
                sTagLevel4.InnerText = map["synopsis"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("searchkeyword");
                if (map.TryGetValue("searchkeyword", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("cliptype");
                sTagLevel4.InnerText = map["cliptype"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("clipcategory");
                sTagLevel4.InnerText = map["clipcategory_name"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("subcategory");
                sTagLevel4.InnerText = map["subcategory"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("targetage");
                sTagLevel4.InnerText = map["targetage"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("hashtag");
                sTagLevel4.InnerText = map["hashtag"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("isfullvod");
                sTagLevel4.InnerText = map["isfullvod"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("broaddate");
                sTagLevel4.InnerText = map["broaddate"].ToString();
                sTagLevel3.AppendChild(sTagLevel4);                
                
                sTagLevel3 = xmlDoc.CreateElement("InstantDescription");                    
                sTagLevel2.AppendChild(sTagLevel3);                

                sTagLevel4 = xmlDoc.CreateElement("match_date");
                if (map.TryGetValue("match_date", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);                

                sTagLevel4 = xmlDoc.CreateElement("searchkeyword");
                if (map.TryGetValue("searchkeyword", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }                
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("sportscategory");
                if (map.TryGetValue("sportskind", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("player");
                if (map.TryGetValue("player", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("team1");
                if (map.TryGetValue("team1", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("team2");
                if (map.TryGetValue("team2", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("inning");
                if (map.TryGetValue("inning", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("mspl_isuse");
                if (map.TryGetValue("mspl_isuse", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("mlb_isuse");
                if (map.TryGetValue("mlb_isuse", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                sTagLevel4 = xmlDoc.CreateElement("sports_sub");
                if (map.TryGetValue("sports_sub", out strValue))
                {
                    sTagLevel4.InnerText = strValue.ToString();
                }
                sTagLevel3.AppendChild(sTagLevel4);

                if (map["clip_YN"].ToString() == "N")
                {
                    sTagLevel4 = xmlDoc.CreateElement("rtmp_url_high");
                    if (map.TryGetValue("rtmp_url", out strValue))
                    {
                        sTagLevel4.InnerText = strValue.ToString();
                    }
                    sTagLevel3.AppendChild(sTagLevel4);                    

                    sTagLevel4 = xmlDoc.CreateElement("rtmp_url_mid");

                    if (map.TryGetValue("rtmp_url_T2", out strValue))
                    {
                        sTagLevel4.InnerText = strValue.ToString();
                    }
                    sTagLevel3.AppendChild(sTagLevel4);

                    sTagLevel4 = xmlDoc.CreateElement("rtmp_url_low");
                    if (map.TryGetValue("rtmp_url_T1", out strValue))
                    {
                        sTagLevel4.InnerText = strValue.ToString();
                    }
                    sTagLevel3.AppendChild(sTagLevel4);

                    sTagLevel4 = xmlDoc.CreateElement("hls_url_high");
                    if (map.TryGetValue("hls_url", out strValue))
                    {
                        sTagLevel4.InnerText = strValue.ToString();
                    }
                    sTagLevel3.AppendChild(sTagLevel4);

                    sTagLevel4 = xmlDoc.CreateElement("hls_url_mid");
                    if (map.TryGetValue("hls_url_T2", out strValue))
                    {
                        sTagLevel4.InnerText = strValue.ToString();
                    }
                    sTagLevel3.AppendChild(sTagLevel4);

                    sTagLevel4 = xmlDoc.CreateElement("hls_url_low");
                    if (map.TryGetValue("hls_url_T1", out strValue))
                    {
                        sTagLevel4.InnerText = strValue.ToString();
                    }
                    sTagLevel3.AppendChild(sTagLevel4);
                }
            
                root.AppendChild(sTag);

                sTag = xmlDoc.CreateElement("MovieDescription");

                sTagLevel1 = xmlDoc.CreateElement("MovieInformationTable");
                sTag.AppendChild(sTagLevel1);

                sTagLevel2 = xmlDoc.CreateElement("mediatype");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel2 = xmlDoc.CreateElement("mediaformat");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel2 = xmlDoc.CreateElement("playtime");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel2 = xmlDoc.CreateElement("filesize");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel2 = xmlDoc.CreateElement("VideoInformation");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel3 = xmlDoc.CreateElement("codec");
                sTagLevel3.InnerText = map["v_codec"].ToString();
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel3 = xmlDoc.CreateElement("bitrate");
                sTagLevel3.InnerText = map["v_bitrate"].ToString();
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel3 = xmlDoc.CreateElement("framerate");
                sTagLevel3.InnerText = "29.97";
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel3 = xmlDoc.CreateElement("ratio");
                sTagLevel3.InnerText = "16:9";
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel2 = xmlDoc.CreateElement("AudioInformation");
                sTagLevel1.AppendChild(sTagLevel2);

                sTagLevel3 = xmlDoc.CreateElement("codec");
                sTagLevel3.InnerText = map["a_codec"].ToString();
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel3 = xmlDoc.CreateElement("bitrate");
                sTagLevel3.InnerText = map["a_bitrate"].ToString();
                sTagLevel2.AppendChild(sTagLevel3);

                sTagLevel3 = xmlDoc.CreateElement("channel");
                sTagLevel3.InnerText = "2 channels";
                sTagLevel2.AppendChild(sTagLevel3);

                root.AppendChild(sTag);
                
            }
            catch (Exception e)
            {
                log.logging(e.ToString());                
            }

            m_xmlDoc = xmlDoc;            
        }        

        public String ConvertJson(XmlDocument doc)
        {
            String returnStr = JsonConvert.SerializeXmlNode(doc);
            return returnStr;
        }

        public XmlDocument ConvertXml(String json)
        {
            XmlDocument doc = new XmlDocument();
            doc = JsonConvert.DeserializeXmlNode(json);
            m_xmlDoc = doc;
            return doc;
        }

        public XmlDocument GetCurrentXmlDocument()
        {
            if (m_xmlDoc != null)
            {
                return m_xmlDoc;
            }
            else
            {
                return null;
            }
        }

        public String GetCurrentJson()
        {
            if (m_strJson != null)
            {
                return m_strJson;
            }
            else
            {
                return null;
            }
        }

        public XmlDocument AppendXml(XmlDocument doc)
        {
            return doc;
        }
    }
}