using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Specialized;
using IntegrationHub.Interfaces.API;
using IntegrationHub.WCF.Contracts;
using System.Collections.Generic;
using Mincom.MineMarket.DAL;


namespace pa.integration.universal.adaptor
{
        #region Get PHD Data

    public class ImportDataFromPHD : IScheduledTask
    {
        #region Private Declarations

        private NameValueCollection mNameValueCollection;
        private const string mErrorSource = "PA Integration AAQ Adaptor - ImportDataFromPHD Class";

        #endregion


        //ANTONY:In the "Constructor" region, the "mNameValueCollection" is initialized with default values. 
        //Each key-value in the collection is used to configure various data import options, such as the PHD server
        //, database name, integrated security, etc.
        #region Constructor

        public ImportDataFromPHD()
        {
            mNameValueCollection = new NameValueCollection();
            mNameValueCollection.Add("01 Source - PHD Server", "");
            mNameValueCollection.Add("02 Source - PHD Database Name", "");
            mNameValueCollection.Add("03 Source - PHD Use Integrated Security", "TRUE");
            mNameValueCollection.Add("04 Source - PHD User", "");
            mNameValueCollection.Add("05 Source - PHD Password", "");
            mNameValueCollection.Add("06 Tags Mapper - Connection Type", "SQLServerNative");
            mNameValueCollection.Add("07 Tags Mapper - Database Server Name", "");
            mNameValueCollection.Add("08 Tags Mapper - Database Name", "");
            mNameValueCollection.Add("09 Tags Mapper - Use Integrated Security", "TRUE");
            mNameValueCollection.Add("10 Tags Mapper - User ID", "");
            mNameValueCollection.Add("11 Tags Mapper - Password", "");
            mNameValueCollection.Add("12 Shift Duration", "8");
            mNameValueCollection.Add("13 Day Duration", "24");
            mNameValueCollection.Add("14 Assay Start Hour", "5");
            mNameValueCollection.Add("15 Shift Start Hour", "6");
            mNameValueCollection.Add("16 Day Start Hour", "6");
            mNameValueCollection.Add("17 Month Start Hour", "6");
            mNameValueCollection.Add("18 Delay (seconds)", "1");
            mNameValueCollection.Add("19 Wait Period (minutes)", "0");
            mNameValueCollection.Add("20 Use MSMQ", "TRUE");
            mNameValueCollection.Add("21 Send Known Values", "FALSE");
            mNameValueCollection.Add("22 Log File", "");
            mNameValueCollection.Add("23 Web Service URL", "https://localhost/PA/LogSheetService.asmx");
            mNameValueCollection.Add("24 Number of Decimals", "-1");
        }
                  
        #endregion
        

        //ANTONY/This code is an implementation of an interface called IScheduledTask. This interface is used to schedule tasks to run regularly at a specific time.
        #region IScheduledTask Members

            bool IScheduledTask.Execute(EventActionArgs args, out AdaptorCallException ex)
            {
                //ANTONY/Modify the variable declarations to reduce the lines of code.
                #region Variable Declaration

                DataTable dataTable = null;
                string pSourceUseIntegratedSecurity = "", pSourcePHDUser = "", pSourcePHDPassword = "", pTagsMapperConnectionType = "",
                    pTagsMapperDatabaseServerName = "", pTagsMapperDatabaseName = "", pTagsMapperUseIntegratedSecurity = "",
                    pTagsMapperUserID = "", pTagsMapperPassword = "", pUseMSMQ = "", pSendKnownValues = "", pLogFile = "",
                    url = "", connectionStringPHD = "";

                int pShiftDuration = 0, pDayDuration = 0, pAssayStartHour = 0, pShiftStartHour = 0, pDayStartHour = 0,
                    pMonthStartHour = 0, pDelay = 0, pWaitPeriod = 0, pNumberDecimals = 0;

                SqlConnection sqlConnectionTagsMapper = null, sqlConnectionPHD = null;
                #endregion


                #region Parameters, Connections and Tags Mappings

                try
                {
                    #region Get Parameters
                    #region Source PHD
                // antony:The operator '??' is being used to assign default values ​​to variables if they are null.
                // Get Source - PHD Server
                string pSourcePHDServer = mNameValueCollection["01 Source - PHD Server"] ?? throw new ArgumentException("The parameter 'Source - PHD Server' must be indicated");
                // Get Source - Database Name
                string pSourceDatabaseName = mNameValueCollection["02 Source - PHD Database Name"] ?? throw new ArgumentException("The parameter 'Source - Database Name' must be indicated");
                //antony:Converts the parameter string to a boolean value, returning true if the conversion is successful and false otherwise.
                // Get Source - Use Integrated Security
                bool.TryParse(mNameValueCollection["03 Source - PHD Use Integrated Security"], out bool pSourceUseIntegratedSecurity);
                    if (!pSourceUseIntegratedSecurity)
                    {
                        throw new ArgumentException("The parameter 'Source - Use Integrated Security' must be TRUE or FALSE");
                    }
                // Get Source - PHD User
                //Antony:an exception is thrown if pSourcePHDPassword or pSourcePHDUser is null or empty
                pSourcePHDUser = mNameValueCollection["04 Source - PHD User"] ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(pSourcePHDUser) || pSourceUseIntegratedSecurity.ToUpper().Trim() == "TRUE")
                {
                    // Get Source - PHD Password
                    pSourcePHDPassword = mNameValueCollection["05 Source - PHD Password"] ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(pSourcePHDPassword) && pSourceUseIntegratedSecurity.ToUpper().Trim() == "FALSE")
                    {
                        throw new ArgumentException("The parameter 'Source - PHD Password' must be indicated");
                    }
                }
                else
                {
                    throw new ArgumentException("The parameter 'Source - PHD User' must be indicated");
                }

                #endregion

                #region Tags Mapper

                // Get Tags Mapper - Connection Type
                pTagsMapperConnectionType = mNameValueCollection["06 Tags Mapper - Connection Type"];
                if (string.IsNullOrEmpty(pTagsMapperConnectionType) || pTagsMapperConnectionType.Trim() == String.Empty)
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - Connection Type' must be indicated");
                }
                if (pTagsMapperConnectionType.ToUpper() != "SQLSERVERNATIVE")
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - Connection Type' only can be assigned to 'SQLServerNative'");
                }

                // Get Tags Mapper - Database Server Name
                pTagsMapperDatabaseServerName = mNameValueCollection["07 Tags Mapper - Database Server Name"];
                if (string.IsNullOrEmpty(pTagsMapperDatabaseServerName) || pTagsMapperDatabaseServerName.Trim() == String.Empty)
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - Database Server Name' must be indicated");
                }

                // Get Tags Mapper - Database Name
                pTagsMapperDatabaseName = mNameValueCollection["08 Tags Mapper - Database Name"];
                if (string.IsNullOrEmpty(pTagsMapperDatabaseName) || pTagsMapperDatabaseName.Trim() == String.Empty)
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - Database Name' must be indicated");
                }

                // Get Tags Mapper - Use Integrated Security
                //antony:the code was reduced
                //Antony:The bool.TryParse() used to parse the boolean value of the parameter and check if it is valid.
                if (!bool.TryParse(mNameValueCollection["09 Tags Mapper - Use Integrated Security"], out bool tagsMapperUseIntegratedSecurity))
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - Use Integrated Security' must be TRUE or FALSE");
                }
                // Get Tags Mapper - User ID
                //The conditions were only combined since it seemed to me that it was very repetitive
                pTagsMapperUserID = mNameValueCollection["10 Tags Mapper - User ID"];
                if (pTagsMapperUseIntegratedSecurity.ToUpper().Trim() != "TRUE" && (string.IsNullOrEmpty(pTagsMapperUserID) || pTagsMapperUserID.Trim() == String.Empty))
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - User ID' must be indicated");
                }

                // Get Tags Mapper - Password
                pTagsMapperPassword = mNameValueCollection["11 Tags Mapper - Password"];
                if (pTagsMapperUseIntegratedSecurity.ToUpper().Trim() != "TRUE"&&(string.IsNullOrEmpty(pTagsMapperPassword) || pTagsMapperPassword.Trim() == String.Empty))
                {
                    throw new ArgumentException("The parameter 'Tags Mapper - Password' must be indicated");
                }

                #endregion

                #region Miscellaneus

                //antony:I reduced lines of code with "||" and "?" This operator before the trim method is to avoid an exception in case it is Null
                if (!int.TryParse(mNameValueCollection["12 Shift Duration"]?.Trim(), out pShiftDuration))
                {
                    throw new ArgumentException("The parameter 'Shift Duration' must be an integer");
                }

                if (!int.TryParse(mNameValueCollection["13 Day Duration"]?.Trim(), out pDayDuration))
                {
                    throw new ArgumentException("The parameter 'Day Duration' must be indicated and be an integer");
                }                
               
                if (!int.TryParse(mNameValueCollection["14 Assay Duration"]?.Trim(), out pAssayStartHour))
                {
                    throw new ArgumentException("The parameter 'Assay Duration' must be indicated and be an integer");
                }
                
                if (!int.TryParse(mNameValueCollection["15 Shift Start Hour"]?.Trim(), out pShiftStartHour))
                {
                    throw new ArgumentException("The parameter 'Shift Start Hour' must be indicated and be an integer");
                }
                
                if (!int.TryParse(mNameValueCollection["16 Day Start Hour"]?.Trim(), out pDayStartHour))
                {
                    throw new ArgumentException("The parameter 'Day Start Hour' must be indicated and be an integer");
                }
                
                if (!int.TryParse(mNameValueCollection["17 Month Start Hour"]?.Trim(), out pMonthStartHour))
                {
                    throw new ArgumentException("The parameter 'Month Start Hour' must be indicated and be an integer");
                }
                
                if (!int.TryParse(mNameValueCollection["18 Delay (seconds)"]?.Trim(), out int pDelay) || pDelay < 0)
                {
                    throw new ArgumentException("The parameter 'Delay'  must be a non-negative integer and must be indicated.");
                }
                
                if (!int.TryParse(mNameValueCollection["19 Wait Period (minutes)"]?.Trim(), out int pWaitPeriod) || pWaitPeriod < -1)
                {
                    throw new ArgumentException("The parameter 'Wait Period' must be a valid integer: -1 (wait until getting a value), 0 (do not wait), or a positive number of minutes to wait");
                }
                // Get Use MSMQ
                if (!bool.TryParse(mNameValueCollection["20 Use MSMQ"]?.Trim(), out bool pUseMSMQ))
                {
                    throw new ArgumentException("The parameter 'Use MSMQ' must be indicated and must be a valid boolean value: TRUE or FALSE");
                }
                // Send Known Values
                if (!bool.TryParse(mNameValueCollection["21 Send Known Values"]?.Trim(), out bool pUseMSMQ))
                {
                    throw new ArgumentException("The parameter 'Send Known Values' must be indicated");
                }
                // Get Log File
                pLogFile = mNameValueCollection["22 Log File"];

                // Get Web Service URL
                //Antony:The 'string.UsNullOrWhite' method was used instead of concatenating two conditions since this method verifies if it is null, empty or contains characters with white spaces.
                if (string.IsNullOrWhiteSpace(mNameValueCollection["23 Web Service URL"]))
                {
                    throw new ArgumentException("The parameter 'Web Service URL' must be indicated");
                }

                // Get Number of Decimals
                if (!int.TryParse(mNameValueCollection["24 Number of Decimals"]?.Trim(), out int pNumberDecimals) || pNumberDecimals < -1)
                {
                    throw new ArgumentException("The parameter 'Number of Decimals' must be indicated and must be -1 (no rounding) or 0 (no decimals) or an integer equal or greater than 1 (N decimals)");
                }
                #endregion
                
                #endregion

                 #region Connect to PHD Server
                 //Antony:This block of code establishes two different database connections, one for the PDH server and one for a tag mapper.

                connectionStringPHD = String.Empty;
                if (pSourceUseIntegratedSecurity.ToUpper() == "TRUE")
                {
                    connectionStringPHD = "Server = " + pSourcePHDServer.Trim() + "; Database = " + pSourceDatabaseName.Trim() + "; Trusted_Connection = true;";

                }
                else
                {
                    connectionStringPHD = "Server = " + pSourcePHDServer.Trim() + "; Database = " + pSourceDatabaseName.Trim() + "; User ID = " + pSourcePHDUser.Trim() + "; Password = " + pSourcePHDPassword.Trim() + ";";

                }
                //sqlConnectionPHD = new SqlConnection(connectionStringPHD);
                //sqlConnectionPHD.Open();
                #endregion
                //Antony:Estoy utilizando un operador ternario para simplificar la asignación de la cadena de conexión, para que sea mas facil leer y entender el codigo
                // string connectionString = pTagsMapperUseIntegratedSecurity.ToUpper() == "TRUE"
                // ? $"Server={pTagsMapperDatabaseServerName.Trim()};Database={pTagsMapperDatabaseName.Trim()};Trusted_Connection=true;"
                // : $"Server={pTagsMapperDatabaseServerName.Trim()};Database={pTagsMapperDatabaseName.Trim()};User ID={pTagsMapperUserID.Trim()};Password={pTagsMapperPassword.Trim()};";
                // sqlConnectionTagsMapper = new SqlConnection(connectionString);
                // sqlConnectionTagsMapper.Open();   
                #region Connect to Tags Mapper

                string connectionString = String.Empty;

                if (pTagsMapperUseIntegratedSecurity.ToUpper() == "TRUE")
                {
                    connectionString = "Server = " + pTagsMapperDatabaseServerName.Trim() + "; Database = " + pTagsMapperDatabaseName.Trim() + "; Trusted_Connection = true;";
                }
                else
                {
                    connectionString = "Server = " + pTagsMapperDatabaseServerName.Trim() + "; Database = " + pTagsMapperDatabaseName.Trim() + "; User ID = " + pTagsMapperUserID.Trim() + "; Password = " + pTagsMapperPassword.Trim() + ";";
                }
                sqlConnectionTagsMapper = new SqlConnection(connectionString);
                sqlConnectionTagsMapper.Open();

                #endregion

                #region Get Tags Mappings

                String queryRead = "Select CalculationID, CalculationDescription, CalculationName, SourceTag, TransactionName, ExternalAttributeID, PeriodType, RetrievalMode, TimeOffset, WriteTime, Factor, Conversion, CalculationBasis, SummaryType, SummaryDuration, SummaryFilter, LastDateReadValue  " +
                                   "FROM LookupPHDToPATable " +
                                   "Order By CalculationID ASC";

                // Create a SqlCommand object and pass the constructor the connection string and the query string.
                SqlCommand sqlCommandRead = new SqlCommand(queryRead, sqlConnectionTagsMapper);

                // Use the above SqlCommand object to create a SqlDataReader object.
                SqlDataReader sqlDataReader = sqlCommandRead.ExecuteReader();

                // Create a DataTable object to hold all the data returned by the query.
                dataTable = new DataTable();

                // Use the DataTable.Load(SqlDataReader) function to put the results of the query into a DataTable.
                dataTable.Load(sqlDataReader);
                sqlCommandRead.Dispose();
                sqlDataReader.Close();

                #endregion

            }
            catch (Exception e)
            {
                if (sqlConnectionTagsMapper != null && sqlConnectionTagsMapper.State == ConnectionState.Open)
                {
                    sqlConnectionTagsMapper.Close();
                }
                ex = new AdaptorCallException(e.Message, e.StackTrace, e, mErrorSource);
                return false;
            }

            #endregion
            //Antony:This region of code contains a loop that iterates through a set of tag mappings, that is, a list of objects that contain information about each tag and how it should be processed.
            //where some operations are performed, such as reading the current value of the label, applying a calculation formula, or converting a value to another format.
            #region Loop trough Tags Mappings

            String errorMessage = String.Empty;

            Global.WriteLog(pLogFile, System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Starting process ..."); // + dataTable.Rows.Count.ToString()) ;
            
            for (int ind = 0; ind < dataTable.Rows.Count; ind++)
            {

                PHDAdaptorTagConfiguration tagConfiguration = new PHDAdaptorTagConfiguration();

                tagConfiguration.CalculationID = (Int32)(dataTable.Rows[ind].ItemArray[0]);
                tagConfiguration.CalculationDescription = dataTable.Rows[ind].ItemArray[1].ToString();
                tagConfiguration.CalculationName = dataTable.Rows[ind].ItemArray[2].ToString();
                tagConfiguration.SourceTag = dataTable.Rows[ind].ItemArray[3].ToString();
                tagConfiguration.TransactionName = dataTable.Rows[ind].ItemArray[4].ToString();
                tagConfiguration.ExternalAttributeID = dataTable.Rows[ind].ItemArray[5].ToString();
                tagConfiguration.PeriodType = dataTable.Rows[ind].ItemArray[6].ToString();
                tagConfiguration.RetrievalMode = dataTable.Rows[ind].ItemArray[7].ToString();
                tagConfiguration.TimeOffset = (String)(dataTable.Rows[ind].ItemArray[8]);
                tagConfiguration.WriteTime = dataTable.Rows[ind].ItemArray[9].ToString();
                tagConfiguration.Factor = Convert.ToDouble((Decimal)(dataTable.Rows[ind].ItemArray[10]));
                tagConfiguration.Conversion = (dataTable.Rows[ind].ItemArray[11] is DBNull ? "" : dataTable.Rows[ind].ItemArray[11].ToString());
                tagConfiguration.CalculationBasis = dataTable.Rows[ind].ItemArray[12].ToString();
                tagConfiguration.SummaryType = dataTable.Rows[ind].ItemArray[13].ToString();
                tagConfiguration.SummaryDuration = dataTable.Rows[ind].ItemArray[14].ToString();
                tagConfiguration.SummaryFilter = dataTable.Rows[ind].ItemArray[15].ToString();
                tagConfiguration.LastDateReadValue = (DateTime)(dataTable.Rows[ind].ItemArray[16]);

                Global.WriteLog(pLogFile, System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " CalculationID: " + tagConfiguration.CalculationID.ToString() + " Description: " + tagConfiguration.CalculationDescription + " SourceTag: " + tagConfiguration.SourceTag.ToString());

                try
                {
                    DateTime timeNow = System.DateTime.MinValue;
                    timeNow = DateTime.Now; 
                    DateTime lastValueTime = System.DateTime.MinValue;
                    DateTime startTime = System.DateTime.MinValue;
                    DateTime endTime = System.DateTime.MinValue;
                    string sstartTime = string.Empty;
                    string sendTime = string.Empty; 

                    #region Retrieval Mode


                    RetrievalTypeConstants retrievalMode = RetrievalTypeConstants.rtAuto;

                    switch (tagConfiguration.RetrievalMode.ToUpper())
                    {
                        case "AUTO":
                            retrievalMode = RetrievalTypeConstants.rtAuto;
                            break;
                        case "AFTER":
                            retrievalMode = RetrievalTypeConstants.rtAfter;
                            break;
                        case "ATORAFTER":
                            retrievalMode = RetrievalTypeConstants.rtAtOrAfter;
                            break;
                        case "ATORBEFORE":
                            retrievalMode = RetrievalTypeConstants.rtAtOrBefore;
                            break;
                        case "BEFORE":
                            retrievalMode = RetrievalTypeConstants.rtBefore;
                            break;
                        case "COMPRESSED":
                            retrievalMode = RetrievalTypeConstants.rtCompressed;
                            break;
                        case "INTERPOLATED":
                            retrievalMode = RetrievalTypeConstants.rtInterpolated;
                            break;
                        default:
                            break;
                    }

                    #endregion

                    //#region Summary Type

                    //ArchiveSummaryTypeConstants summaryType = ArchiveSummaryTypeConstants.astAverage;
                    //ArchiveSummariesTypeConstants summaryType2 = ArchiveSummariesTypeConstants.asAverage;

                    //switch (tagConfiguration.SummaryType.ToUpper())
                    //{
                    //    case "AVERAGE":
                    //        summaryType = ArchiveSummaryTypeConstants.astAverage;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asAverage;
                    //        break;
                    //    case "COUNT":
                    //        summaryType = ArchiveSummaryTypeConstants.astCount;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asCount;
                    //        break;
                    //    case "MAXIMUM":
                    //        summaryType = ArchiveSummaryTypeConstants.astMaximum;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asMaximum;
                    //        break;
                    //    case "MEAN":
                    //        summaryType = ArchiveSummaryTypeConstants.astMean;
                    //        break;
                    //    case "MINIMUM":
                    //        summaryType = ArchiveSummaryTypeConstants.astMinimum;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asMinMaxRange;
                    //        break;
                    //    case "POPULATION STANDARD DEVIATION":
                    //        summaryType = ArchiveSummaryTypeConstants.astPStdDev;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asPStdDev;
                    //        break;
                    //    case "RANGE":
                    //        summaryType = ArchiveSummaryTypeConstants.astRange;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asMinMaxRange;
                    //        break;
                    //    case "STANDARD DEVIATION":
                    //        summaryType = ArchiveSummaryTypeConstants.astStdDev;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asStdDev;
                    //        break;
                    //    case "TOTAL":
                    //        summaryType = ArchiveSummaryTypeConstants.astTotal;
                    //        summaryType2 = ArchiveSummariesTypeConstants.asTotal;
                    //        break;
                    //    default:
                    //        break;
                    //}

                    //#endregion

                    //#region Calculation Basis

                    //CalculationBasisConstants calculationBasis = CalculationBasisConstants.cbEventWeighted;

                    //switch (tagConfiguration.CalculationBasis.ToUpper())
                    //{
                    //    case "EVENT WEIGHTED":
                    //        calculationBasis = CalculationBasisConstants.cbEventWeighted;
                    //        break;
                    //    case "EVENT WEIGHTED EXCLUDE EARLIEST EVENT":
                    //        calculationBasis = CalculationBasisConstants.cbEventWeightedExcludeEarliestEvent;
                    //        break;
                    //    case "EVENT WEIGHTED EXCLUDE MOST RECENT EVENT":
                    //        calculationBasis = CalculationBasisConstants.cbEventWeightedExcludeMostRecentEvent;
                    //        break;
                    //    case "EVENT WEIGHTED INCLUDE BOTH ENDS":
                    //        calculationBasis = CalculationBasisConstants.cbEventWeightedIncludeBothEnds;
                    //        break;
                    //    case "TIME WEIGHTED":
                    //        calculationBasis = CalculationBasisConstants.cbTimeWeighted;
                    //        break;
                    //    case "TIME WEIGHTED CONTINUOUS":
                    //        calculationBasis = CalculationBasisConstants.cbTimeWeightedContinuous;
                    //        break;
                    //    case "TIME WEIGHTED DISCRETE":
                    //        calculationBasis = CalculationBasisConstants.cbTimeWeightedDiscrete;
                    //        break;
                    //    default:
                    //        break;
                    //}

                    //#endregion

                    sstartTime = GlobalPHD.CalculateStartTime(tagConfiguration.LastDateReadValue, tagConfiguration.PeriodType);
                    sendTime = GlobalPHD.CalculateEndTime(tagConfiguration.LastDateReadValue, tagConfiguration.PeriodType, pDayDuration, pShiftDuration);
                    startTime = Convert.ToDateTime(sstartTime);
                    endTime = Convert.ToDateTime(sendTime);

                    Boolean continueCycle = true;

                    while (timeNow > endTime)  // && continueCycle)
                    {
                        Global.WriteLog(pLogFile, System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Continue cycle ... endTime: " + endTime.ToString("yyyy-MM-dd HH:mm:ss"));

                        String calculationResult = String.Empty;
                        DateTime extendedEndTime = endTime.AddMinutes(pWaitPeriod);


                        List<PHDData> PHDValuesList = new List<PHDData>();
                        NullableDecimal  PHDValue = NullableDecimal.Null;
                        if (tagConfiguration.RetrievalMode.ToUpper() == "EXACTAT")
                        {
                            string err = string.Empty;
                            PHDValuesList = Exe_spPHDDATA(connectionStringPHD, tagConfiguration.SourceTag, "", startTime, endTime, 0, out err);
                            if (err.Length > 0)
                                errorMessage = errorMessage + " " + err;

                            if (PHDValuesList.Count > 0)
                            {
                                for (int i = PHDValuesList.Count - 1; i >= 0; i--)
                                {
                                    if (DateTime.Compare(tagConfiguration.LastDateReadValue, PHDValuesList[i].Timestamp) == 0)
                                    {
                                        PHDValue = PHDValuesList[i].Value;
                                        break; 
                                    }
                                }
                            }

                        }
                        else
                        {
                            if (tagConfiguration.RetrievalMode.ToUpper() == "INTHESHIFT")
                            {
                                DateTime endShiftDateTime = endTime;
                                DateTime startShiftDateTime = startTime;

                                string err = string.Empty;
                                PHDValuesList = Exe_spPHDDATA(connectionStringPHD, tagConfiguration.SourceTag, "", startTime, endTime, 0, out err);//startShiftDateTime.ToString(), endShiftDateTime.ToString(), 0, out err);
                                if (err.Length > 0)
                                    errorMessage = errorMessage + " " + err;

                                if (PHDValuesList.Count > 0)
                                {
                                    //PHDValue = PHDValuesList[PHDValuesList.Count].Value;
                                    for (int i = 0 ; i < PHDValuesList.Count ; i++)
                                    {
                                        if (startShiftDateTime <= PHDValuesList[i].Timestamp && endShiftDateTime > PHDValuesList[i].Timestamp)
                                        {
                                            PHDValue = PHDValuesList[i].Value;
                                        }
                                    }


                                }
                                //piValuesList = piPoint.Data.RecordedValues(startShiftDateTime, endShiftDateTime, BoundaryTypeConstants.btInside);
                                //if (piValuesList.Count > 0)
                                //{
                                //    piValue = piValuesList[piValuesList.Count];
                                //}
                            }
                            else
                            {
                                if (tagConfiguration.CalculationName.ToUpper() == "SPPHDATA")
                                {
                                    //piValue = piPoint.Data.ArcValue(GlobalPI.GetTime(startTime, endTime, tagConfiguration.TimeOffset), retrievalMode);
                                    DateTime endShiftDateTime = endTime;
                                    DateTime startShiftDateTime = startTime;

                                    string err = string.Empty;
                                    PHDValuesList = Exe_spPHDDATA(connectionStringPHD, tagConfiguration.SourceTag, "", startTime, endTime, 0, out err);//startShiftDateTime.ToString(), endShiftDateTime.ToString(), 0, out err);
                                    if (err.Length > 0)
                                        errorMessage = errorMessage + " " + err;

                                    if (PHDValuesList.Count > 0)
                                    {
                                        for (int i  = 0; i < PHDValuesList.Count; i++)
                                        {
                                            if (startShiftDateTime <= PHDValuesList[i].Timestamp && endShiftDateTime > PHDValuesList[i].Timestamp)
                                            {
                                                PHDValue = PHDValuesList[i].Value;
                                            }
                                        }
                                    }

                                }
                                else
                                {
                                    if (tagConfiguration.CalculationName.ToUpper() == "SUMMARY")
                                    {
                                        //DateTime endShiftDateTime = GlobalPI.GetTime(startTime, endTime, tagConfiguration.TimeOffset);
                                        //endShiftDateTime.AddMilliseconds(-1 * endShiftDateTime.Millisecond).AddTicks(-1 * endShiftDateTime.Ticks);
                                        //DateTime startShiftDateTime = endShiftDateTime;
                                        //startShiftDateTime = startShiftDateTime.AddHours(-1 * pShiftDuration).AddSeconds(1);
                                        //endShiftDateTime = endShiftDateTime.AddMilliseconds(999).AddTicks(9999);
                                        //piValue = piPoint.Data.Summary(startShiftDateTime, endShiftDateTime, summaryType, calculationBasis);
                                    }
                                    else
                                    {
                                        if (tagConfiguration.CalculationName.ToUpper() == "FILTEREDSUMMARY")
                                        {
                                        //    DateTime endShiftDateTime = GlobalPI.GetTime(startTime, endTime, tagConfiguration.TimeOffset);
                                        //    endShiftDateTime.AddMilliseconds(-1 * endShiftDateTime.Millisecond).AddTicks(-1 * endShiftDateTime.Ticks);
                                        //    DateTime startShiftDateTime = endShiftDateTime;
                                        //    startShiftDateTime = startShiftDateTime.AddHours(-1 * pShiftDuration).AddSeconds(1);
                                        //    endShiftDateTime = endShiftDateTime.AddMilliseconds(999).AddTicks(9999);
                                        //    IPIData2 iPId2 = (IPIData2)piPoint.Data;
                                        //    NamedValues summaryValues = iPId2.FilteredSummaries(startShiftDateTime, endShiftDateTime, tagConfiguration.SummaryDuration, tagConfiguration.SummaryFilter, summaryType2, calculationBasis);
                                        //    if (summaryValues.Count >= 0 && ((PIValues)summaryValues[summaryType2.ToString().Substring(2)].Value).Count >= 0)
                                        //    {
                                        //        piValue = ((PIValues)summaryValues[summaryType2.ToString().Substring(2)].Value)[((PIValues)summaryValues[summaryType2.ToString().Substring(2)].Value).Count];
                                        //    }
                                        }
                                    }
                                }
                            }
                        }

                        // Get data from PHD and convert it


                        if (PHDValue.IsNull)
                        {
                            calculationResult = String.Empty;
                        }
                        else
                        {
                            calculationResult = ConvertValue(tagConfiguration.Conversion, PHDValue.Value.ToString());
                        }


                        Double valDouble = 0;

                        if (!double.TryParse(calculationResult, out valDouble))
                        {

                            // Condition added to control the case when a value is missing.
                            //  0 --> No wait for the value
                            // -1 --> Wait until a value is published
                            //  N --> Wait N minutes for the value

                            if (pWaitPeriod == 0 || (pWaitPeriod > 0 && timeNow > extendedEndTime))
                            {
                                if (PHDValue.IsNull)
                                {
                                    if (tagConfiguration.RetrievalMode.ToUpper() == "EXACTAT")
                                    {
                                        errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Tag '" + tagConfiguration.SourceTag + "' has been rejected because it doesn't have a value at " + GlobalPHD.GetTime(startTime, endTime, tagConfiguration.TimeOffset).ToString("yyyy/MM/dd HH:mm:ss") + "\r\n\r\n";
                                    }
                                    else
                                    {
                                        errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Tag '" + tagConfiguration.SourceTag + "' has been rejected because it doesn't have a value in the shift " + GlobalPHD.GetTime(startTime, endTime, tagConfiguration.TimeOffset).AddHours(-1 * pShiftDuration).AddSeconds(1).ToString("yyyy/MM/dd HH:mm:ss") + " to " + GlobalPHD.GetTime(startTime, endTime, tagConfiguration.TimeOffset).ToString("yyyy/MM/dd HH:mm:ss") + "\r\n\r\n";
                                    }
                                }
                                else
                                {
                                    //if (piValue.Value is PISDK.DigitalState)
                                    //{
                                    //    errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Tag '" + tagConfiguration.SourceTag + "' has been rejected because at " + GlobalPI.GetTime(startTime, endTime, tagConfiguration.TimeOffset).ToString("yyyy/MM/dd HH:mm:ss") + " its value is a digital state '" + ((PISDK.DigitalState)(piValue.Value)).Name + "' and its converted value '" + calculationResult + "' is not numeric.\r\n\r\n";
                                    //}
                                    //else
                                    //{
                                    //    errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Tag '" + tagConfiguration.SourceTag + "' has been rejected because at " + GlobalPI.GetTime(startTime, endTime, tagConfiguration.TimeOffset).ToString("yyyy/MM/dd HH:mm:ss") + " its value '" + piValue.Value + "' and its converted value '" + calculationResult + "' are not numeric.\r\n\r\n";
                                    //}
                                }
                            }
                            calculationResult = String.Empty;



                        }

                        if (calculationResult != String.Empty && (pWaitPeriod == 0 || pWaitPeriod == -1 || (pWaitPeriod > 0 && timeNow > extendedEndTime) || Convert.ToBoolean(pSendKnownValues)))
                        {

                            DateTime timeToWrite = System.DateTime.MinValue;

                            if (tagConfiguration.WriteTime.ToUpper().IndexOf("START") >= 0)
                            {
                                if (tagConfiguration.WriteTime.ToUpper() == "START")
                                {

                                    timeToWrite = startTime;
                                }
                                else
                                {
                                    timeToWrite = startTime.AddMinutes(int.Parse(tagConfiguration.WriteTime.ToUpper().Replace("START", "").Trim()));
                                }
                            }
                            else
                            {
                                if (tagConfiguration.WriteTime.ToUpper().IndexOf("END") >= 0)
                                {
                                    if (tagConfiguration.WriteTime.ToUpper() == "END")
                                    {
                                        timeToWrite = endTime;
                                    }
                                    else
                                    {
                                        timeToWrite = endTime.AddMinutes(int.Parse(tagConfiguration.WriteTime.ToUpper().Replace("END", "").Trim()));
                                    }
                                }
                            }
                            //antony/This code will format a numeric calculation result with a specific number of decimal places. 
                            //The format only applies if the pNumberDecimals parameter is greater than or equal to zero and if the calculationResult string can be parsed as a double.

                            #region Format the value

                            if (pNumberDecimals > -1)
                            {
                                double auxDouble = 0;
                                if (double.TryParse(calculationResult.Trim(), out auxDouble))
                                {
                                    string format = "#,##0";
                                    if (pNumberDecimals > 0)
                                    {
                                        format = format + ".".PadRight(pNumberDecimals + 1, '0');
                                    }
                                    calculationResult = (Math.Round(auxDouble, pNumberDecimals)).ToString(format);
                                }
                            }

                            #endregion

                            #region Create Message Data
                            //antony:creates a message data array with a single row and ten columns, populates it with multiple values, sends the message to a destination via MSMQ or LogSheetService depending on the value of the pUseMSMQ parameter.
                            String[,] dataPA = new String[1, 10];
                            dataPA[0, Global.PATransaction.SetID] = tagConfiguration.TransactionName;
                            dataPA[0, Global.PATransaction.LogTime] = timeToWrite.ToString("yyyy-MM-dd HH:mm:ss");
                            dataPA[0, Global.PATransaction.Category] = "Quality";
                            dataPA[0, Global.PATransaction.AttributeID] = tagConfiguration.ExternalAttributeID;
                            dataPA[0, Global.PATransaction.UOM] = "";
                            dataPA[0, Global.PATransaction.Value] = calculationResult;
                            dataPA[0, Global.PATransaction.UserID] = System.Security.Principal.WindowsIdentity.GetCurrent().Name;
                            dataPA[0, Global.PATransaction.Comment] = "";
                            dataPA[0, Global.PATransaction.Result] = "";
                            dataPA[0, Global.PATransaction.RowID] = "";

                            // Send the message
                            //antony:If pUseMSMQ is true, creates a transaction object using the CreateTransaction method of the Global class and submits it to PA using the SubmitTransaction method with a delay specified by the pDelay parameter. 
                            //If pUseMSMQ is false,it sends the message to PA using the WriteLogsheetService method of the Global class,
                            // then checks the result value for each row in the message data array and concatenates any error messages in the errorMessage variable.
                            if (Convert.ToBoolean(pUseMSMQ))
                            {
                                Global.WriteLog(pLogFile, "MSMQ");
                                // Send message to PA using a message queue

                                Transaction transaction = Global.CreateTransaction(dataPA);
                                Global.SubmitTransaction(transaction, pDelay);
                            }
                            else
                            {
                                // Send message to PA using LogSheetService

                                Global.WriteLogsheetService(url, ref dataPA);

                                for (int indexControl = 0; indexControl < dataPA.GetLength(0); indexControl++)
                                {
                                    string vResult = dataPA[indexControl, Global.PATransaction.Result];
                                    if (vResult != "TRUE")
                                    {
                                        errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Error calculating source tag '" + tagConfiguration.SourceTag + "' with calculation '" + tagConfiguration.CalculationName + "' and transaction name '" + tagConfiguration.TransactionName + "' and external attribute id '" + tagConfiguration.ExternalAttributeID + "'.\r\n\r\n" + vResult + "\r\n\r\n";
                                    }
                                }

                                // Write log file
                                //Antony: if the pLogFile parameter specifies a log file path, it writes the message data to the log file using the WriteLog method of the Global class.
                                if (pLogFile != "")
                                {
                                    Global.WriteLog(pLogFile, dataPA);
                                }
                            }

                            #endregion

                        }


                        // Juan
                        // Condition added to control the case when a value is missing.
                        //  0 --> No wait for the value
                        // -1 --> Wait until a value is published
                        //  N --> Wait N minutes for the value

                        if (pWaitPeriod == 0 || (calculationResult != String.Empty && pWaitPeriod == -1) || (pWaitPeriod > 0 && timeNow > extendedEndTime))
                        {

                            #region Prepare times for next cicle

                            tagConfiguration.LastDateReadValue = endTime;
                            sstartTime = GlobalPHD.CalculateStartTime(tagConfiguration.LastDateReadValue, tagConfiguration.PeriodType);
                            sendTime = GlobalPHD.CalculateEndTime(tagConfiguration.LastDateReadValue, tagConfiguration.PeriodType, pDayDuration, pShiftDuration);
                            startTime = Convert.ToDateTime(sstartTime);
                            endTime = Convert.ToDateTime(sendTime);
                            #endregion

                            #region Update Last Date Read Value

                            String queryUpdate = "update LookupPHDToPATable set LastDateReadValue = '" + tagConfiguration.LastDateReadValue.ToString("yyyy/MM/dd HH:mm:ss") + "' where CalculationID = " + tagConfiguration.CalculationID;

                            // Create a SqlCommand object and pass the constructor the connection string and the query string.
                            SqlCommand sqlCommandUpdate = new SqlCommand(queryUpdate, sqlConnectionTagsMapper);

                            // Use the above SqlCommand object to create a SqlDataReader object.
                            int rowsAffected = sqlCommandUpdate.ExecuteNonQuery();

                            sqlCommandUpdate.Dispose();

                            Global.WriteLog(pLogFile, System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + queryUpdate);

                            #endregion

                            continueCycle = true;

                        }
                        else
                        {
                            if (calculationResult != String.Empty && pWaitPeriod > 0)
                            {
                                sstartTime = GlobalPHD.CalculateStartTime(endTime, tagConfiguration.PeriodType);
                                sendTime = GlobalPHD.CalculateEndTime(endTime, tagConfiguration.PeriodType, pDayDuration, pShiftDuration);
                                startTime = Convert.ToDateTime(sstartTime);
                                endTime = Convert.ToDateTime(sendTime);

                                continueCycle = true;
                                Global.WriteLog(pLogFile, System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " continueCicle = true - News startTime: " + startTime.ToString("yyyy/MM/dd HH:mm:ss") + " endTime: " + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                            }
                            else
                            {
                                sstartTime = GlobalPHD.CalculateStartTime(endTime, tagConfiguration.PeriodType);
                                sendTime = GlobalPHD.CalculateEndTime(endTime, tagConfiguration.PeriodType, pDayDuration, pShiftDuration);
                                startTime = Convert.ToDateTime(sstartTime);
                                endTime = Convert.ToDateTime(sendTime);

                                continueCycle = false;
                                Global.WriteLog(pLogFile, System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " continueCicle = false");
                            }
                        }

                    }

                }
                catch (Exception e)
                {
                    errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " Error calculating source tag '" + tagConfiguration.SourceTag + "' with calculation '" + tagConfiguration.CalculationName + "' and transaction name '" + tagConfiguration.TransactionName + "' and external attribute id '" + tagConfiguration.ExternalAttributeID + "'. A possible cause is that the tag does not exist.\r\n\r\n" + e.Message + "\r\n\r\n";
                }
                finally
                {
                }
            }

            #endregion

            #region Release connections
            //Antony:This try-catch block is used to close database connections if they are open and to handle any exceptions that may occur when closing connections.
            try
            {

                #region Close Connections

                if (sqlConnectionTagsMapper != null && sqlConnectionTagsMapper.State == ConnectionState.Open)
                {
                    sqlConnectionTagsMapper.Close();
                }

                if (sqlConnectionPHD != null && sqlConnectionPHD.State == ConnectionState.Open)
                {
                    sqlConnectionPHD.Close();
                }

                #endregion

                ex = null;
            }
            catch (Exception e)
            {
                ex = new AdaptorCallException(e.Message, e.StackTrace, e, mErrorSource);
                return false;
            }

            #endregion

            #region Write to Log
            //antony:The code checks if a string variable called errorMessage is not empty.
            // If it is not empty, it checks if its length is greater than 30,000 characters. 
            //If it is, it shortens the string to the first 30,000 characters using the Substring method.
            // If the error message is longer than a certain length, it is truncated before being written to the log.
            if (errorMessage != String.Empty)
            {
                if (errorMessage.Length > 30000)
                {
                    errorMessage = errorMessage.Substring(0, 30000);
                }
                ex = new AdaptorCallException(errorMessage, null, null, mErrorSource);
                return false;
            }

            #endregion

            return true;

        }

        NameValueCollection IScheduledTask.GetInitializationStrings()
        {
            return mNameValueCollection;
        }

        void IScheduledTask.Initialize(NameValueCollection values)
        {
            mNameValueCollection = values;
        }

        #endregion

        #region Private Methods

        public List<PHDData> Exe_spPHDDATA(string strConnection, string TagNames, string Unit, DateTime StartDate, DateTime EndDate, int Interval, out string errorMessage)
        {
            // Variables locales
            List<PHDData> lista = new List<PHDData>();
            int codError = 0;
            int returnValue = 0;
            errorMessage = string.Empty;

            // antony:The using statement is used to ensure the destruction of objects and free resources
            //errorMessage = strConnection;
            using (SqlConnection con = new SqlConnection(strConnection))
            {
                using (SqlCommand com = new SqlCommand())
                {
                    try
                    {
                        // Al comando se le asigna una conexión
                        com.Connection = con;

                        // Se le indica el tipo de comando y el nombre
                        com.CommandType = CommandType.StoredProcedure;
                        com.CommandText = "PHDINT.dbo.spPHDData";

                        // Se añaden los parámetros de entrada
                        com.Parameters.AddWithValue("@phdlinkedserver", "PHD");
                        com.Parameters.AddWithValue("@tagnames", TagNames);
                        com.Parameters.AddWithValue("@unit", Unit);
                        com.Parameters.AddWithValue("@sdate", StartDate.ToString("yyyy-MM-dd HH:mm:ss"));
                        com.Parameters.AddWithValue("@edate", EndDate.ToString("yyyy-MM-dd HH:mm:ss"));
                        com.Parameters.AddWithValue("@interval", Interval);

                        // Se añaden los parámetros de salida y se crean variables para facilitar su recuperacion
                        //SqlParameter paramOutCodError = com.Parameters.Add("@codError", SqlDbType.Int, int.MaxValue);
                        //paramOutCodError.Direction = ParameterDirection.Output;
                        SqlParameter paramReturned = com.Parameters.Add("@return_value", SqlDbType.Int, int.MaxValue);
                        paramReturned.Direction = ParameterDirection.ReturnValue;

                        // Se abre la conexión
                        con.Open();

                        // Se recupera el lector de datos al utilizar ExecuteReader
                        SqlDataReader lector = com.ExecuteReader();


                        // Mientras no terminer de leer filas ejecuta recupera la información obtenida
                        while (lector.Read())
                        {
                            // Creamos un objeto con los parámetros obtenidos de la consulta
                            PHDData fila = new PHDData()
                            {
                                TagName = lector["TAGNAME"] != DBNull.Value ? (string)lector["TAGNAME"] : string.Empty,
                                Units = lector["UNITS"] != DBNull.Value ? (string)lector["UNITS"] : string.Empty,
                                Timestamp = lector["TIMESTAMP"] != DBNull.Value ? Convert.ToDateTime(lector["TIMESTAMP"]) : DateTime.MinValue,
                                Value = lector["VALUE"] != DBNull.Value ? Convert.ToDecimal( Double.Parse((string)lector["VALUE"],System.Globalization.NumberStyles.Float)) : 0,//DBNull.Value ? Convert.ToDecimal(lector["VALUE"]) : 0,
                                Confidence = lector["CONFIDENCE"] != DBNull.Value ? Convert.ToInt32(lector["CONFIDENCE"]) : 0
                            };
                            decimal d = lector["VALUE"] != DBNull.Value ? Convert.ToDecimal(Double.Parse((string)lector["VALUE"], System.Globalization.NumberStyles.Float)) : 0;
                            // Añadimos la fila al listado
                            lista.Add(fila);
                        }

                        // Se cierrra el lector de datos para poder recuperar los parámetros de salida
                        lector.Close();

                        // Se recuperan los parámetros de salida
                        //codError = (paramOutCodError.Value != null && paramOutCodError.Value != DBNull.Value) ? (int)paramOutCodError.Value : 0;
                        //returnValue = (paramOutCodError.Value != null && paramReturned.Value != DBNull.Value) ? (int)paramReturned.Value : 0;
                        returnValue = (paramReturned.Value != DBNull.Value) ? (int)paramReturned.Value : 0;

                    }
                    catch (Exception ex)
                    {
                        //Console.WriteLine("ERROR : " + ex.Message);
                        errorMessage = errorMessage + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "ERROR Exe_spPHDData : " + ex.Message;
                    }
                    finally
                    {
                        // Nos aseguramos de cerrar la conexión en caso de error
                        con.Close();
                    }
                }
            }

            // Variable para poner un punto de parada de depuración y ver los resultados
            int parada = lista.Count;
            return lista;
        }


        #region Convert Value

        private String ConvertValue(String conversion, String value)
        {
            if (conversion != null && conversion != "")
            {
                String[] conversionList = conversion.Split('@');
                for (int i = 0; i < conversionList.Length; i++)
                {
                    String[] conversionItem = conversionList[i].Split(',');
                    if (value.IndexOf(conversionItem[0]) >= 0)
                    {
                        if (conversionItem[1].ToUpper().Trim() == "E")
                        {
                            value = conversionItem[2];
                        }
                        else
                        {
                            value = value.Replace(conversionItem[0], conversionItem[2]);
                        }
                    }
                }
            }

            return value;
        }

        #endregion

        #endregion
    }

    #endregion
}