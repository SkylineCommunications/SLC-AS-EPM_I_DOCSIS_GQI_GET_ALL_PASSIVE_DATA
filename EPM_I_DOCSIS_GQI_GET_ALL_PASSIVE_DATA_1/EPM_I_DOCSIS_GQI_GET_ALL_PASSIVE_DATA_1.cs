/*
****************************************************************************
*  Copyright (c) 2024,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

19/03/2024	1.0.0.1		GBO, Skyline	Initial version
****************************************************************************
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Messages;

[GQIMetaData(Name = "All Passives Data")]
public class PassiveData : IGQIDataSource, IGQIInputArguments, IGQIOnInit
{
    private readonly GQIStringArgument frontEndElementArg = new GQIStringArgument("FE Element")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument filterEntityArg = new GQIStringArgument("Filter Entity")
    {
        IsRequired = false,
    };

    private readonly GQIStringArgument entityBeTablePidArg = new GQIStringArgument("BE Entity Table PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument filterBeTablePidArg = new GQIStringArgument("BE Filter Table PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument latitudeIdxArg = new GQIStringArgument("Latitude IDX")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument longitudeIdxArg = new GQIStringArgument("Longitude IDX")
    {
        IsRequired = true,
    };

    private GQIDMS _dms;

    private string frontEndElement = String.Empty;

    private string filterEntity = String.Empty;

    private int entityBeTablePid = 0;

    private int filterBeTablePid = 0;

    private int latitudeIdx = 0;

    private int longitudeIdx = 0;

    private List<GQIRow> listGqiRows = new List<GQIRow> { };

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[]
        {
            frontEndElementArg,
            filterEntityArg,
            entityBeTablePidArg,
            filterBeTablePidArg,
            latitudeIdxArg,
            longitudeIdxArg,
        };
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
            new GQIStringColumn("Entity Name"),
            new GQIDoubleColumn("Latitude"),
            new GQIDoubleColumn("Longitude"),
            new GQIIntColumn("Highest Severity"),
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        return new GQIPage(listGqiRows.ToArray())
        {
            HasNextPage = false,
        };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        listGqiRows.Clear();
        try
        {
            frontEndElement = args.GetArgumentValue(frontEndElementArg);
            filterEntity = args.GetArgumentValue(filterEntityArg);
            entityBeTablePid = Convert.ToInt32(args.GetArgumentValue(entityBeTablePidArg));
            filterBeTablePid = Convert.ToInt32(args.GetArgumentValue(filterBeTablePidArg));
            latitudeIdx = Convert.ToInt32(args.GetArgumentValue(latitudeIdxArg));
            longitudeIdx = Convert.ToInt32(args.GetArgumentValue(longitudeIdxArg));

            var backEndHelper = GetBackEndElement();
            if (backEndHelper == null)
            {
                return new OnArgumentsProcessedOutputArgs();
            }

            var entityTable = GetTable(backEndHelper.ElementId, filterBeTablePid, new List<string>
            {
                String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityBeTablePid + 1, backEndHelper.EntityId),
            });

            var entityRows = ExtractPassiveData(entityTable, latitudeIdx, longitudeIdx);
            var activeAlarms = GetActiveAlarms();
            AddHighestSeverity(entityRows, activeAlarms);

            CreatePassiveLocationTable(entityRows);
        }
        catch
        {
            listGqiRows = new List<GQIRow>();
        }

        return new OnArgumentsProcessedOutputArgs();
    }

    public List<HelperPartialSettings[]> GetTable(string element, int tableId, List<string> filter)
    {
        var columns = new List<HelperPartialSettings[]>();

        var elementIds = element.Split('/');
        if (elementIds.Length > 1 && Int32.TryParse(elementIds[0], out int dmaId) && Int32.TryParse(elementIds[1], out int elemId))
        {
            // Retrieve client connections from the DMS using a GetInfoMessage request
            var getPartialTableMessage = new GetPartialTableMessage(dmaId, elemId, tableId, filter.ToArray());
            var paramChange = (ParameterChangeEventMessage)_dms.SendMessage(getPartialTableMessage);

            if (paramChange != null && paramChange.NewValue != null && paramChange.NewValue.ArrayValue != null)
            {
                columns = paramChange.NewValue.ArrayValue
                    .Where(av => av != null && av.ArrayValue != null)
                    .Select(p => p.ArrayValue.Where(v => v != null)
                    .Select(c => new HelperPartialSettings
                    {
                        CellValue = c.CellValue.InteropValue,
                        DisplayValue = c.CellValue.CellDisplayValue,
                        DisplayType = c.CellDisplayState,
                    }).ToArray()).ToList();
            }
        }

        return columns;
    }

    public AlarmEventMessage[] GetActiveAlarms()
    {
        DMSMessage[] responses = _dms.SendMessages(new GetActiveAlarmsMessage());
        var firstMessage = (ActiveAlarmsResponseMessage)responses.FirstOrDefault();
        if (firstMessage == null)
            return new AlarmEventMessage[0];

        return firstMessage.ActiveAlarms;
    }

    public BackEndHelper GetBackEndElement()
    {
        if (String.IsNullOrEmpty(filterEntity))
        {
            return null;
        }

        var backendTable = GetTable(frontEndElement, 1200500, new List<string>
        {
            "forceFullTable=true",
        });

        if (backendTable != null && backendTable.Any())
        {
            for (int i = 0; i < backendTable[0].Count(); i++)
            {
                var key = Convert.ToString(backendTable[0][i].CellValue);

                var backendEntityTable = GetTable(key, entityBeTablePid, new List<string>
                {
                    String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityBeTablePid + 2, filterEntity),
                });

                if (backendEntityTable != null && backendEntityTable.Any() && backendEntityTable[0].Length > 0)
                {
                    return new BackEndHelper
                    {
                        ElementId = key,
                        EntityId = Convert.ToString(backendEntityTable[0][0].CellValue),
                    };
                }
            }
        }

        return null;
    }

    private static void AddHighestSeverity(List<EntityOverview> entityRows, AlarmEventMessage[] activeAlarms)
    {
        var alarmValues = new Dictionary<string, int>
            {
                { "Normal", 1 },
                { "Warning", 2 },
                { "Minor", 3 },
                { "Major", 4 },
                { "Critical", 5 },
            };

        var systemNameAlarms = new Dictionary<string, List<int>>();
        foreach (var alarm in activeAlarms)
        {
            if (!alarmValues.ContainsKey(alarm.Severity))
            {
                continue;
            }

            foreach (var alarmProperty in alarm.Properties)
            {
                if (alarmProperty.Name != "System Name")
                {
                    continue;
                }

                if (!systemNameAlarms.ContainsKey(alarmProperty.Value))
                {
                    systemNameAlarms[alarmProperty.Value] = new List<int>();
                }

                systemNameAlarms[alarmProperty.Value].Add(alarmValues[alarm.Severity]);
            }
        }

        foreach (var entity in entityRows)
        {
            var highestSeverity = 1;
            if (systemNameAlarms.ContainsKey(entity.EntityName))
            {
                highestSeverity = systemNameAlarms[entity.EntityName].Max();
            }

            entity.HighestSeverity = highestSeverity;
        }
    }

    private static List<EntityOverview> ExtractPassiveData(List<HelperPartialSettings[]> entityTable, int latitudeIdx, int longitudeIdx)
    {
        List<EntityOverview> entityRows = new List<EntityOverview>();
        if (entityTable != null && entityTable.Any())
        {
            for (int i = 0; i < entityTable[0].Count(); i++)
            {
                var name = Convert.ToString(entityTable[1][i].CellValue);
                var entityRow = new EntityOverview
                {
                    EntityName = name,
                    Latitude = Convert.ToString(entityTable[latitudeIdx][i].CellValue),
                    Longitude = Convert.ToString(entityTable[longitudeIdx][i].CellValue),
                };

                entityRows.Add(entityRow);
            }
        }

        return entityRows;
    }

    private void CreatePassiveLocationTable(List<EntityOverview> entityRows)
    {
        foreach (var entityRow in entityRows)
        {
            List<GQICell> listGqiCells = new List<GQICell>
                {
                    new GQICell
                    {
                        Value = entityRow.EntityName,
                    },
                    new GQICell
                    {
                        Value = Convert.ToDouble(entityRow.Latitude, CultureInfo.InvariantCulture),
                    },
                    new GQICell
                    {
                        Value = Convert.ToDouble(entityRow.Longitude, CultureInfo.InvariantCulture),
                    },
                    new GQICell
                    {
                        Value = entityRow.HighestSeverity,
                    },
                };

            var gqiRow = new GQIRow(listGqiCells.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }
}

public class BackEndHelper
{
    public string ElementId { get; set; }

    public string CcapId { get; set; }

    public string CollectorId { get; set; }

    public string EntityId { get; set; }
}

public class HelperPartialSettings
{
    public object CellValue { get; set; }

    public object DisplayValue { get; set; }

    public ParameterDisplayType DisplayType { get; set; }
}

public class EntityOverview
{
    public string EntityName { get; set; }

    public string Latitude { get; set; }

    public string Longitude { get; set; }

    public int HighestSeverity { get; set; }
}