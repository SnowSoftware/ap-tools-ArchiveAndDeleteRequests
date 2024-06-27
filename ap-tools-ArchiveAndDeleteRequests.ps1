# Removed require to support SQLPS module as well (without TrustServerCertificate)
# #Requires -Modules @{ ModuleName="SqlServer"; ModuleVersion="22.0.0" }
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $APDatabaseServer,

    [Parameter(Mandatory)]
    [ValidateScript({
        $inputDate = $_
        $twoYearsAgo = (Get-Date).AddYears(-2)
        if ($inputDate -lt $twoYearsAgo) {
            $true
        } else {
            throw "ArchiveOnlyRequestsOlderThanThis must be older than 2 years"
        }
    })]
    [datetime]
    $ArchiveOnlyRequestsOlderThanThis,

    [Parameter(Mandatory)]
    [ValidateScript({
        $inputAmount = $_
        if ($inputAmount -le 100) {
            $true
        } else {
            throw "HowManyRequestsToArchiveAndDelete must be 100 or less"
        }
    })]
    [Int]
    $HowManyRequestsToArchiveAndDelete,

    [string]
    $ArchivePath = '.\',

    [switch]
    $UnsafeMode,

    [switch]
    $DontUseTrustServerCertificate
    
)

function Validate-AP-sql-db-access {
        try {
            if ($DontUseTrustServerCertificate) {
                $SQLAccess = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query "SELECT TOP 1 CompanyName FROM [SnowAutomationPlatformDomain].[dbo].[LicenseKeys]" -Verbose -ErrorAction Stop
            }
            else {
                $SQLAccess = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query "SELECT TOP 1 CompanyName FROM [SnowAutomationPlatformDomain].[dbo].[LicenseKeys]" -TrustServerCertificate -Verbose -ErrorAction Stop
            }
        }
        catch {
            Write-Error "Failed to connect to DB. Exception: $($PSItem.Exception.Message)"
            Exit
        }
        Write-Information "Database successfully queried."
}

function Setup-ArchivePathFolder {
    $batchname = "ManualRequestsArchive" + (get-date -Format FileDateTimeUniversal)
    try {
        $ArchiveFolder = New-Item -Path $($ArchivePath + '\') -Name $batchname -ItemType Directory

    } catch {
        Write-Error "Could not create archive folder. Exception: $($PSItem.Exception.Message)"
        exit
    }

    return $ArchiveFolder
}

function Read-APtable {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Requests',
                    'ServiceInstance_Requests',
                    'RequestStatusLogs',
                    'RequestUpdates',
                    'ServiceInstances',
                    'InstanceAttributes',
                    'RequestActivities',
                    'RequestActivityStatusLogs',
                    'RequestParameters',
                    'ActivityLogs',
                    'RequestParameterMappings'
                    )]
        [string]$Table
    )

    switch ($Table) {
        Requests {
          $Query = "SELECT top $HowManyRequestsToArchiveAndDelete r.* FROM Requests r 
            where RequestingUserID != 'MISSITO_SCHEDULED_TASK'
	        and DateCreated < '$ArchiveOnlyRequestsOlderThanThis'
	        order by DateCreated asc"
        }

        ServiceInstance_Requests {
            $Query = "SELECT sir.* FROM ServiceInstance_Requests sir 
            where sir.Request_Id in ($($Requests.id -join ','))"
        }
        RequestStatusLogs {
            $Query = "SELECT rsl.* FROM RequestStatusLogs rsl 
            where rsl.RequestId in ($($Requests.id -join ','))"
        }
        RequestUpdates {
            $Query = "SELECT ru.* FROM RequestUpdates ru 
            where ru.Request_Id in ($($Requests.id -join ','))"
        }
        ServiceInstances {
            $Query = "SELECT si.* FROM ServiceInstances si 
            where si.Id in ($(($ServiceInstance_Requests.ServiceInstance_Id | Where-Object { -not [string]::IsNullOrEmpty($_) }) -join ','))"
        }
        InstanceAttributes {
            $Query = "SELECT ia.* FROM InstanceAttributes ia 
            where ia.ServiceInstance_Id in ($(($ServiceInstance_Requests.ServiceInstance_Id | Where-Object { -not [string]::IsNullOrEmpty($_) }) -join ','))"
        }
        RequestActivities {
            $Query = "SELECT ra.* FROM RequestActivities ra 
            where ra.ServiceInstance_Request_Id in ($($ServiceInstance_Requests.Id -join ','))"
        }
        RequestActivityStatusLogs {
            $Query = "SELECT rasl.* FROM RequestActivityStatusLogs rasl 
            where rasl.RequestActivityId in ($($RequestActivities.Id -join ','))"
        }
        RequestParameters {
            $Query = "SELECT rp.* FROM RequestParameters rp 
            where rp.RequestActivity_Id in ($($RequestActivities.Id -join ','))"
        }
        ActivityLogs {
            $Query = "SELECT al.* FROM ActivityLogs al 
            where al.RequestActivity_Id in ($($RequestActivities.Id -join ','))"
        }
        RequestParameterMappings {
            $Query = "SELECT rpm.* FROM RequestParameterMappings rpm 
            where rpm.RequestParameterId in ($($RequestParameters.Id -join ','))"
        }

        Default {
            Write-Error "No Table defined."
            exit
        }
    }



    try {
        if ($DontUseTrustServerCertificate) {
            $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -Verbose -ErrorAction Stop
        }
        else {
            $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
        }
    }
    catch {
        Write-Error "Failed to read to DB. Exception: $($PSItem.Exception.Message)"
        Exit
    }

    return $BatchQueryResult




}

Function Export-APTableArchive {
    param(
        $TableObject,

        [Parameter(Mandatory)]
        [ValidateSet('Requests',
                    'ServiceInstance_Requests',
                    'RequestStatusLogs',
                    'RequestUpdates',
                    'ServiceInstances',
                    'InstanceAttributes',
                    'RequestActivities',
                    'RequestActivityStatusLogs',
                    'RequestParameters',
                    'ActivityLogs',
                    'RequestParameterMappings'
                    )]
        [string]$Table,
        [switch]$IsSchema
    )

    if ($TableObject.Count -le 0) {
        Write-Information "Table $Table is empty, no export."
        return
    }

    if ($IsSchema) { 
        $OutFilename = "$($ArchiveFolder.FullName)\Archived_$($Table)Schema_" + (get-date -Format FileDateTimeUniversal) + ".csv"
    } else {
        $OutFilename = "$($ArchiveFolder.FullName)\Archived_$($Table)_" + (get-date -Format FileDateTimeUniversal) + ".csv"
    }

    

    try {
        $TableObject | Export-Csv -Path $OutFilename -NoClobber -Encoding UTF8 -NoTypeInformation -Delimiter "," -Append -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to write RequestParameters to archive file. Exception: $($PSItem.Exception.Message)"
        Exit
    }
    return $OutFilename
}

function Read-APtableSchema {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Requests',
                    'ServiceInstance_Requests',
                    'RequestStatusLogs',
                    'RequestUpdates',
                    'ServiceInstances',
                    'InstanceAttributes',
                    'RequestActivities',
                    'RequestActivityStatusLogs',
                    'RequestParameters',
                    'ActivityLogs',
                    'RequestParameterMappings'
                    )]
        [string]$Table
    )

    $Query = "SELECT 
        TABLE_NAME AS 'Table Name',
        COLUMN_NAME AS 'Column Name',
        DATA_TYPE AS 'Data Type',
        CHARACTER_MAXIMUM_LENGTH AS 'Max Length',
        IS_NULLABLE AS 'Is Nullable'
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = '$Table';    
        "


            try {
                if ($DontUseTrustServerCertificate) {
                    $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -Verbose -ErrorAction Stop
                }
                else {
                    $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
                }
            }
            catch {
                Write-Error "Failed to read to DB. Exception: $($PSItem.Exception.Message)"
                Exit
            }
        
            return $BatchQueryResult


}

#region MAIN SCRIPT
try {

    Validate-AP-sql-db-access
    
    $ArchiveFolder = Setup-ArchivePathFolder
    
    # Requests
    $Requests = Read-APtable -Table Requests
    $RequestsSchema = Read-APtableSchema -Table Requests
    Export-APTableArchive -TableObject $Requests -Table Requests
    Export-APTableArchive -TableObject $RequestsSchema -Table Requests -IsSchema
    
    # ServiceInstance_Requests
    $ServiceInstance_Requests = Read-APtable -Table ServiceInstance_Requests
    $ServiceInstance_RequestsSchema = Read-APtableSchema -Table ServiceInstance_Requests
    Export-APTableArchive -TableObject $ServiceInstance_Requests -Table ServiceInstance_Requests
    Export-APTableArchive -TableObject $ServiceInstance_RequestsSchema -Table ServiceInstance_Requests -IsSchema

    # RequestStatusLogs
    $RequestStatusLogs = Read-APtable -Table RequestStatusLogs
    $RequestStatusLogsSchema = Read-APtableSchema -Table RequestStatusLogs
    Export-APTableArchive -TableObject $RequestStatusLogs -Table RequestStatusLogs
    Export-APTableArchive -TableObject $RequestStatusLogsSchema -Table RequestStatusLogs -IsSchema

    # RequestUpdates
    $RequestUpdates = Read-APtable -Table RequestUpdates
    $RequestUpdatesSchema = Read-APtableSchema -Table RequestUpdates
    Export-APTableArchive -TableObject $RequestUpdates -Table RequestUpdates
    Export-APTableArchive -TableObject $RequestUpdatesSchema -Table RequestUpdates -IsSchema
    
    # ServiceInstances
    $ServiceInstances = Read-APtable -Table ServiceInstances
    $ServiceInstancesSchema = Read-APtableSchema -Table ServiceInstances
    Export-APTableArchive -TableObject $ServiceInstances -Table ServiceInstances
    Export-APTableArchive -TableObject $ServiceInstancesSchema -Table ServiceInstances -IsSchema

    # InstanceAttributes
    $InstanceAttributes = Read-APtable -Table InstanceAttributes
    $InstanceAttributesSchema = Read-APtableSchema -Table InstanceAttributes
    Export-APTableArchive -TableObject $InstanceAttributes -Table InstanceAttributes
    Export-APTableArchive -TableObject $InstanceAttributesSchema -Table InstanceAttributes -IsSchema

    # RequestActivities
    $RequestActivities = Read-APtable -Table RequestActivities
    $RequestActivitiesSchema = Read-APtableSchema -Table RequestActivities
    Export-APTableArchive -TableObject $RequestActivities -Table RequestActivities
    Export-APTableArchive -TableObject $RequestActivitiesSchema -Table RequestActivities -IsSchema

    # RequestActivityStatusLogs
    $RequestActivityStatusLogs = Read-APtable -Table RequestActivityStatusLogs
    $RequestActivityStatusLogsSchema = Read-APtableSchema -Table RequestActivityStatusLogs
    Export-APTableArchive -TableObject $RequestActivityStatusLogs -Table RequestActivityStatusLogs
    Export-APTableArchive -TableObject $RequestActivityStatusLogsSchema -Table RequestActivityStatusLogs -IsSchema

    # RequestParameters
    $RequestParameters = Read-APtable -Table RequestParameters
    $RequestParametersSchema = Read-APtableSchema -Table RequestParameters
    Export-APTableArchive -TableObject $RequestParameters -Table RequestParameters
    Export-APTableArchive -TableObject $RequestParametersSchema -Table RequestParameters -IsSchema

    # ActivityLogs
    $ActivityLogs = Read-APtable -Table ActivityLogs
    $ActivityLogsSchema = Read-APtableSchema -Table ActivityLogs
    Export-APTableArchive -TableObject $ActivityLogs -Table ActivityLogs
    Export-APTableArchive -TableObject $ActivityLogsSchema -Table ActivityLogs -IsSchema

    # RequestParameterMappings
    $RequestParameterMappings = Read-APtable -Table RequestParameterMappings
    $RequestParameterMappingsSchema = Read-APtableSchema -Table RequestParameterMappings
    Export-APTableArchive -TableObject $RequestParameterMappings -Table RequestParameterMappings
    Export-APTableArchive -TableObject $RequestParameterMappingsSchema -Table RequestParameterMappings -IsSchema
    
} catch {
    Write-Error "General error archiving. Exception: $($PSItem.Exception.Message)"
    exit
}

#region Delete
$confirm = $false
While (($confirm -ne 'ConfirmDelete' -and $Confirm -ne 'Abort') -and -not $UnsafeMode) {
    $confirm = Read-Host "Confirm files are archived. Type [ConfirmDelete] to continue to delete from DB. Type [Abort] to abort. Input"
}

if ($confirm -eq 'Abort') {
    Write-Information "User aborted delete from DB step."
    exit
}



#endregion

Write-Information "Successfully Done."

#endregion END SCRIPT