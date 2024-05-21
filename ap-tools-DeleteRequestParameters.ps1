[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $APDatabaseServer,

    [Parameter(Mandatory)]
    [datetime]
    $DeleteBeforeThisDate,

    [Parameter(Mandatory)]
    [Int]
    $RequestParameterBatchSize,

    [Parameter(Mandatory)]
    [Int]
    $BatchesToRun
    
)


Function Validate-Requirements {

    # Validate DeleteBeforeThisDate is older than 180 days
    $ValidDeleteBeforeThisDate = (Get-Date).AddDays(-180)
    if ($DeleteBeforeThisDate -gt $ValidDeleteBeforeThisDate) {
        Write-Error "DeleteBeforeThisDate needs to be older than 180 days."
        Exit
    }
    Write-Information "DeleteBeforeThisDate is older than 180 days."

    # Validate workload and batch sizes
    $TotalBatchSize = $RequestParameterBatchSize * $BatchesToRun
    if ($TotalBatchSize -ge 50000) {
        Write-Error "Total amount of Request Parameters to run is greater or equal to 50 000. Reduce [RequestParameterBatchSize] and/or [BatchesToRun]."
        Exit
    }
    Write-Information "Total amount of Request Parameters to run is less than 50 000."

    
    # Validate SQL Access
    try {
        $SQLAccess = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query "SELECT TOP 1 CompanyName FROM [SnowAutomationPlatformDomain].[dbo].[LicenseKeys]" -TrustServerCertificate -Verbose -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to connect to DB. Exception: $($PSItem.Exception.Message)"
        Exit
    }
    Write-Information "Database successfully queried."


}

Function Read-RequestParameters {


    $Query = "Select TOP $RequestParameterBatchSize RP.*,R.DateCreated as RequestDateCreated, r.RequestingUserID from RequestParameters RP
    inner join RequestActivities RA on rp.RequestActivity_Id = ra.Id
    inner join ServiceInstance_Requests SIR on ra.ServiceInstance_Request_Id = sir.Id
    inner join Requests R on sir.Request_Id = r.Id
    Where r.DateCreated < '$DeleteBeforeThisDate'
    AND r.RequestingUserID != 'MISSITO_SCHEDULED_TASK'"

    try {
        $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to read to DB. Exception: $($PSItem.Exception.Message)"
        Exit
    }

    return $BatchQueryResult

}
Function Read-RequestParameterMappings {

    param(
        $BatchToReadRequestParameterMappings
    )

    $Query = "Select * from RequestParameterMappings where RequestParameterId in ($($BatchToReadRequestParameterMappings.id -join ','))"

    try {
        $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to read to DB. Exception: $($PSItem.Exception.Message)"
        Exit
    }

    return $BatchQueryResult

}

Function Export-ArchivedRequestParameters {
    param(
        $RequestParametersToArchive
    )

    $OutFilename = "ArchivedRequestParameters" + (get-date -Format FileDateTimeUniversal) + ".csv"

    try {
        $RequestParametersToArchive | Export-Csv -Path .\$OutFilename -NoClobber -Encoding UTF8 -NoTypeInformation -Delimiter ";" -Append -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to write RequestParameters to archive file. Exception: $($PSItem.Exception.Message)"
        Exit
    }
    return $OutFilename
}

Function Export-ArchivedRequestParameterMappings {
    param(
        $RequestParameterMappingsToArchive
    )

    $OutFilename = "ArchivedRequestParameterMappings" + (get-date -Format FileDateTimeUniversal) + ".csv"

    try {
        $RequestParameterMappingsToArchive | Export-Csv -Path .\$OutFilename -NoClobber -Encoding UTF8 -NoTypeInformation -Delimiter ";" -Append -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to write RequestParameterMappings to archive file. Exception: $($PSItem.Exception.Message)"
        Exit
    }
    return $OutFilename
}

Function Delete-RequestParameters {
    param(
        $RequestParametersToDelete
    )
        
        
    $Query = "delete from RequestParameters where Id in ($($RequestParametersToDelete.id -join ','))"
        
    try {
        $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to invoke sql cmd and delete RequestParameters. Exception: $($PSItem.Exception.Message)"
        Exit
    }
    

}

Function Delete-RequestParameterMappings {
    param(
        $BatchToDeleteRequestParameterMappings
    )

    $Query = "delete from RequestParameterMappings where RequestParameterId in ($($BatchToDeleteRequestParameterMappings.id -join ','))"
        
    try {
        $BatchQueryResult = Invoke-Sqlcmd -ServerInstance $APDatabaseServer -Database SnowAutomationPlatformDomain -Query $Query -TrustServerCertificate -Verbose -ErrorAction Stop
    }
    catch {
        Write-Error "Failed to invoke sql cmd and delete RequestParameterMappings. Exception: $($PSItem.Exception.Message)"
        Exit
    }


}

#region Main Script
Write-Verbose "Script starting"
Validate-Requirements

# Run through the batches
for ($i = 1; $i -le $BatchesToRun; $i++) {
    
    # Read DB

    $batch = Read-RequestParameters
    if ($null -eq $batch) {
        Write-Information "No more RequestParameters within scope to read"
        break
    }
    Write-Information "Batch $i/$BatchesToRun read."

    $batchMappings = Read-RequestParameterMappings -BatchToReadRequestParameterMappings $batch
    Write-Information "Batch $i/$BatchesToRun mappings read."

    
    # Export to file

    $OutFilename = Export-ArchivedRequestParameters -RequestParametersToArchive $batch
    Write-Information "RequestParameters file [$OutFilename] written to disk."
    
    if ($null -ne $batchMappings) {
        $OutFilename = Export-ArchivedRequestParameterMappings -RequestParameterMappingsToArchive $batchMappings
        Write-Information "RequestParameterMappings file [$OutFilename] written to disk."
    }

    # Delete from DB

    if ($null -ne $batchMappings) {
        Delete-RequestParameterMappings -BatchToDeleteRequestParameterMappings $batch
        Write-Information "RequestParameterMappings batch $i/$BatchesToRun deleted."
    }

    #Delete-RequestParameters : Failed to read to DB. Exception: The DELETE statement conflicted with the REFERENCE constraint "FK_RequestParameterMappings_RequestParameters". The conflict occurred in database "SnowAutomationPlatformDomain", table "dbo.RequestParameterMappings", column 'RequestParameterId'.
    Delete-RequestParameters -RequestParametersToDelete $batch
    Write-Information "RequestParameters batch $i/$BatchesToRun deleted."
    
    
}
Write-Information "Batches completed"
Write-Verbose "Script completed"
#endregion