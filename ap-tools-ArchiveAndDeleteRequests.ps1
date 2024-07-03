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
            }
            else {
                throw "ArchiveOnlyRequestsOlderThanThis must be older than 2 years"
            }
        })]
    [datetime]
    $ArchiveOnlyRequestsOlderThanThis,

    [Parameter(Mandatory)]
    [ValidateScript({
            $inputAmount = $_
            if ($inputAmount -le 300) {
                $true
            }
            else {
                throw "HowManyRequestsToArchiveAndDelete must be 300 or less"
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
#region Support functions
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
        # Write-Error "Failed to connect to DB. Exception: $($PSItem.Exception.Message)"
        throw $PSItem.Exception
    }
    Write-Information "Database successfully queried."
}

function Setup-ArchivePathFolder {
    $batchname = "ManualRequestsArchive" + (get-date -Format FileDateTimeUniversal)
    try {
        $ArchiveFolder = New-Item -Path $($ArchivePath + '\') -Name $batchname -ItemType Directory -ErrorAction Stop
    }
    catch {
        # Write-Error "Could not create archive folder. Exception: $($PSItem.Exception.Message)"
        throw $PSItem.Exception
    }

    Write-Host "ArchiveFolder: [$($ArchiveFolder.FullName)]"

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
            and Status != 0
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
        # Batch management because RequestParameters are often more than allowed by SQL in "exists in" array statement


                    # Define batch size
                    $batchSize = 40000

                    # Calculate number of batches
                    $batchCount = [Math]::Ceiling($RequestParameters.Count / $batchSize)

                    $queries = @()

                    for ($i = 0; $i -lt $batchCount; $i++) {
                        # Get the current batch
                        $start = $i * $batchSize
                        $end = $start + $batchSize - 1

                        $queries += "SELECT rpm.* FROM RequestParameterMappings rpm 
                                    where rpm.RequestParameterId in ($($RequestParameters[$start..$end].Id -join ','))"

                    }

            # $Query = "SELECT rpm.* FROM RequestParameterMappings rpm 
            # where rpm.RequestParameterId in ($($RequestParameters.Id -join ','))"
        }

        Default {
            Write-Error "No Table defined."
            throw "No Table defined for Read-APTable."
        }
    }



    try {
        if ($Table -eq 'RequestParameterMappings') {

            $BatchQueryResult = @()
            foreach($Query in $queries) {
                if ($DontUseTrustServerCertificate) {
                    $BatchQueryResult += Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -Verbose -ErrorAction Stop
                }
                else {
                    $BatchQueryResult += Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
                }
            }


        } else {

            if ($DontUseTrustServerCertificate) {
                $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -Verbose -ErrorAction Stop
            }
            else {
                $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
            }
        }
    }
    catch {
        throw "Failed to read to DB. Exception: $($PSItem.Exception.Message)"
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
    }
    else {
        $OutFilename = "$($ArchiveFolder.FullName)\Archived_$($Table)_" + (get-date -Format FileDateTimeUniversal) + ".csv"
    }

    

    try {
        $TableObject | Export-Csv -Path $OutFilename -NoClobber -Encoding UTF8 -NoTypeInformation -Delimiter "," -Append -ErrorAction Stop
    }
    catch {
        throw "Failed to write RequestParameters to archive file. Exception: $($PSItem.Exception.Message)"
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

function Read-APTableArchiveFile {
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
        $Table
    )

    $TableArchive = Get-ChildItem -Path $ArchiveFolder.FullName | Where-Object { $_.Name -match "Archived_$($Table)_.*\.csv" -and $_.Name -notmatch 'Schema' }
    
    if ($TableArchive.Count -gt 1) {
        Write-Error "more than one archive file found for $Table."
        exit
    }


    if ($TableArchive.Count -eq 1) {
        return Import-Csv -Path $TableArchive.FullName -Delimiter ','
    }

    return $false

}

function Delete-FromAPTable {
    [CmdletBinding()]
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
        $Table,

        [Parameter(Mandatory)]
        $Ids
    )


    # Define batch size
    $batchSize = 10000

    # Calculate number of batches
    $batchCount = [Math]::Ceiling($Ids.Count / $batchSize)

    for ($i = 0; $i -lt $batchCount; $i++) {
        # Get the current batch
        $start = $i * $batchSize
        $end = $start + $batchSize - 1
        $batchIds = $Ids[$start..$end]

      
        $Query = "delete from $Table where Id in ($($batchIds -join ','))"
        
        if ($Table -eq 'ServiceInstances') {
            $Query = "DELETE FROM ServiceInstances
            WHERE Id IN ($($batchIds -join ',')) AND Id NOT IN (
            SELECT ServiceInstance_Id FROM ServiceInstance_Requests where ServiceInstance_Id IS NOT NULL
            );"
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
            throw "Failed to invoke sql cmd and delete Ids from table. Exception: $($PSItem.Exception.Message)"
            Exit
        }
    
    }
    
    
    Write-Warning "[$Table] got [$($Ids.count)] ID deleted"
    
}
#endregion

#region Main functions
function Archive-APTableManualProcess {
    try {
        Invoke-Command {
        
            # Requests
            $Requests = Read-APtable -Table Requests
            $RequestsSchema = Read-APtableSchema -Table Requests
            $null = Export-APTableArchive -TableObject $Requests -Table Requests
            $null = Export-APTableArchive -TableObject $RequestsSchema -Table Requests -IsSchema

            if ($Requests.Count -le 0) { 
                # Write-Host "No Requests in scope."
                throw "No Requests in scope."
            }

            # ServiceInstance_Requests
            $ServiceInstance_Requests = Read-APtable -Table ServiceInstance_Requests
            $ServiceInstance_RequestsSchema = Read-APtableSchema -Table ServiceInstance_Requests
            $null = Export-APTableArchive -TableObject $ServiceInstance_Requests -Table ServiceInstance_Requests
            $null = Export-APTableArchive -TableObject $ServiceInstance_RequestsSchema -Table ServiceInstance_Requests -IsSchema

            # RequestStatusLogs
            $RequestStatusLogs = Read-APtable -Table RequestStatusLogs
            $RequestStatusLogsSchema = Read-APtableSchema -Table RequestStatusLogs
            $null = Export-APTableArchive -TableObject $RequestStatusLogs -Table RequestStatusLogs
            $null = Export-APTableArchive -TableObject $RequestStatusLogsSchema -Table RequestStatusLogs -IsSchema

            # RequestUpdates
            $RequestUpdates = Read-APtable -Table RequestUpdates
            $RequestUpdatesSchema = Read-APtableSchema -Table RequestUpdates
            $null = Export-APTableArchive -TableObject $RequestUpdates -Table RequestUpdates
            $null = Export-APTableArchive -TableObject $RequestUpdatesSchema -Table RequestUpdates -IsSchema
    
            if ($ServiceInstance_Requests.Count -le 0) { return }

            # ServiceInstances
            if (($ServiceInstance_Requests.ServiceInstance_Id | Where-Object { -not [string]::IsNullOrEmpty($_) }).Count -gt 0) {

                $ServiceInstances = Read-APtable -Table ServiceInstances
                $ServiceInstancesSchema = Read-APtableSchema -Table ServiceInstances
                $null = Export-APTableArchive -TableObject $ServiceInstances -Table ServiceInstances
                $null = Export-APTableArchive -TableObject $ServiceInstancesSchema -Table ServiceInstances -IsSchema
                
                # InstanceAttributes
                $InstanceAttributes = Read-APtable -Table InstanceAttributes
                $InstanceAttributesSchema = Read-APtableSchema -Table InstanceAttributes
                $null = Export-APTableArchive -TableObject $InstanceAttributes -Table InstanceAttributes
                $null = Export-APTableArchive -TableObject $InstanceAttributesSchema -Table InstanceAttributes -IsSchema
            }
                
        
            # RequestActivities
            $RequestActivities = Read-APtable -Table RequestActivities
            $RequestActivitiesSchema = Read-APtableSchema -Table RequestActivities
            $null = Export-APTableArchive -TableObject $RequestActivities -Table RequestActivities
            $null = Export-APTableArchive -TableObject $RequestActivitiesSchema -Table RequestActivities -IsSchema
        
            if ($RequestActivities.Count -le 0) { return }
        
            # RequestActivityStatusLogs
            $RequestActivityStatusLogs = Read-APtable -Table RequestActivityStatusLogs
            $RequestActivityStatusLogsSchema = Read-APtableSchema -Table RequestActivityStatusLogs
            $null = Export-APTableArchive -TableObject $RequestActivityStatusLogs -Table RequestActivityStatusLogs
            $null = Export-APTableArchive -TableObject $RequestActivityStatusLogsSchema -Table RequestActivityStatusLogs -IsSchema
    
            # RequestParameters
            $RequestParameters = Read-APtable -Table RequestParameters
            $RequestParametersSchema = Read-APtableSchema -Table RequestParameters
            $null = Export-APTableArchive -TableObject $RequestParameters -Table RequestParameters
            $null = Export-APTableArchive -TableObject $RequestParametersSchema -Table RequestParameters -IsSchema
        
            # ActivityLogs
            $ActivityLogs = Read-APtable -Table ActivityLogs
            $ActivityLogsSchema = Read-APtableSchema -Table ActivityLogs
            $null = Export-APTableArchive -TableObject $ActivityLogs -Table ActivityLogs
            $null = Export-APTableArchive -TableObject $ActivityLogsSchema -Table ActivityLogs -IsSchema
        
            if ($RequestParameters.Count -le 0) { return }
    
            # RequestParameterMappings
            $RequestParameterMappings = Read-APtable -Table RequestParameterMappings
            $RequestParameterMappingsSchema = Read-APtableSchema -Table RequestParameterMappings
            $null = Export-APTableArchive -TableObject $RequestParameterMappings -Table RequestParameterMappings
            $null = Export-APTableArchive -TableObject $RequestParameterMappingsSchema -Table RequestParameterMappings -IsSchema
        }
    
    }
    catch {
        # Write-Error "General error archiving. Exception: $($PSItem.Exception.Message)"
        throw $PSItem.Exception
    }
    Write-Information "Successfully archived."

}

function Delete-APTableManualProcess {
    Try {
        # validate path to archive folder
        while (-not $(Test-Path $ArchiveFolder -IsValid -ErrorAction SilentlyContinue)) {
            Write-Host "Archive folder not set, or does not exist."
            $NewArchiveFolder = Read-Host -Prompt "Please provide path to archive folder"
            $ArchiveFolder = Get-Item $NewArchiveFolder -ErrorAction SilentlyContinue
        }

        # validate files in archive folder
        if (-not $UnsafeMode) {
            Get-ChildItem -Path $ArchiveFolder.FullName
        }

        $confirm = $false
        While (($confirm -ne 'ConfirmDelete' -and $Confirm -ne 'Abort') -and -not $UnsafeMode) {
            $confirm = Read-Host "Confirm the archived files are correct and should now be deleted. Type [ConfirmDelete] to continue to delete from DB. Type [Abort] to abort. Input"
    
            if ($confirm -eq 'Abort') {
                Throw "User aborted delete from DB step."
                exit
            }
        }

        # User confirm to continue with delete
        $confirm = $false
        While (($confirm -ne 'ConfirmDelete' -and $Confirm -ne 'Abort') -and -not $UnsafeMode) {
            $confirm = Read-Host "Type [ConfirmDelete] to continue to delete from DB. Type [Abort] to abort. Input"

            if ($confirm -eq 'Abort') {
                throw "User aborted delete from DB step."
                exit
            }
        }

        # Do Delete

        # $ArchiveFiles = Get-ChildItem -Path $ArchiveFolder.FullName | Where-Object { $_.Name -match 'Archived_.*_.*\.csv' -and $_.Name -notmatch 'Schema' }

        # RequestParameterMappings
        $RequestParameterMappingsArchive = Read-APTableArchiveFile -Table RequestParameterMappings
        if ($RequestParameterMappingsArchive) {
            Delete-FromAPTable -Table RequestParameterMappings -Ids $RequestParameterMappingsArchive.Id
        }

        # ActivityLogs
        $ActivityLogsArchive = Read-APTableArchiveFile -Table ActivityLogs
        if ($ActivityLogsArchive) {
            Delete-FromAPTable -Table ActivityLogs -Ids $ActivityLogsArchive.Id
        }

        # RequestParameters
        $RequestParametersArchive = Read-APTableArchiveFile -Table RequestParameters
        if ($RequestParametersArchive) {
            Delete-FromAPTable -Table RequestParameters -Ids $RequestParametersArchive.Id
        }

        # RequestActivityStatusLogs
        $RequestActivityStatusLogsArchive = Read-APTableArchiveFile -Table RequestActivityStatusLogs
        if ($RequestActivityStatusLogsArchive) {
            Delete-FromAPTable -Table RequestActivityStatusLogs -Ids $RequestActivityStatusLogsArchive.Id
        }

        # RequestActivities
        $RequestActivitiesArchive = Read-APTableArchiveFile -Table RequestActivities
        if ($RequestActivitiesArchive) {
            Delete-FromAPTable -Table RequestActivities -Ids $RequestActivitiesArchive.Id
        }

        # InstanceAttributes
        $InstanceAttributesArchive = Read-APTableArchiveFile -Table InstanceAttributes
        if ($InstanceAttributesArchive) {
            Delete-FromAPTable -Table InstanceAttributes -Ids $InstanceAttributesArchive.Id
        }

        # ServiceInstance_Requests
        $ServiceInstance_RequestsArchive = Read-APTableArchiveFile -Table ServiceInstance_Requests
        if ($ServiceInstance_RequestsArchive) {
            Delete-FromAPTable -Table ServiceInstance_Requests -Ids $ServiceInstance_RequestsArchive.Id
        }

        # ServiceInstances
        $ServiceInstancesArchive = Read-APTableArchiveFile -Table ServiceInstances
        if ($ServiceInstancesArchive) {
            Delete-FromAPTable -Table ServiceInstances -Ids $ServiceInstancesArchive.Id -ErrorAction Stop
        }

        # RequestUpdates
        $RequestUpdatesArchive = Read-APTableArchiveFile -Table RequestUpdates
        if ($RequestUpdatesArchive) {
            Delete-FromAPTable -Table RequestUpdates -Ids $RequestUpdatesArchive.Id
        }

        # RequestStatusLogs
        $RequestStatusLogsArchive = Read-APTableArchiveFile -Table RequestStatusLogs
        if ($RequestStatusLogsArchive) { 
            Delete-FromAPTable -Table RequestStatusLogs -Ids $RequestStatusLogsArchive.Id
        }

        # Requests
        $RequestsArchive = Read-APTableArchiveFile -Table Requests
        if ($RequestsArchive) {
            Delete-FromAPTable -Table Requests -Ids $RequestsArchive.Id
        }

        Write-Information "Successfully Deleted."
    
    }
    catch {
        throw "General error deleting. Exception: $($PSItem.Exception.Message)"
        exit
    }
}
#endregion



#region MAIN SCRIPT

try {

    Validate-AP-sql-db-access
    
    $ArchiveFolder = Setup-ArchivePathFolder
    
    Archive-APTableManualProcess
    
    Delete-APTableManualProcess
    
    Write-Information "Script completed."

    return "Script completed."

} catch {
    if ($PSItem.Exception.Message -eq 'No Requests in scope.') {
        throw $PSItem.Exception.Message
    }
    # Write-Error "Error thrown. Exception: $($PSItem.Exception.Message)"
    throw $PSItem.Exception
}

#endregion END SCRIPT