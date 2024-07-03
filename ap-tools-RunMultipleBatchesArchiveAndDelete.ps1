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
    $DontUseTrustServerCertificate,

    [int]
    $batchCount
    
)

for ($i = 1; $i -le $batchCount; $i++) {
    try {
        Write-Host "Running Batch $i / $batchCount"
        $result = . '.\ap-tools-ArchiveAndDeleteRequests.ps1' -APDatabaseServer $APDatabaseServer -ArchiveOnlyRequestsOlderThanThis $ArchiveOnlyRequestsOlderThanThis -HowManyRequestsToArchiveAndDelete $HowManyRequestsToArchiveAndDelete -UnsafeMode:$UnsafeMode -DontUseTrustServerCertificate:$DontUseTrustServerCertificate -InformationAction:$InformationPreference
    } catch {
        if ($PSItem.Exception.Message -eq 'No Requests in scope.') {
            Write-Host $PSItem.Exception.Message
            break
        }
        Write-Error $PSItem.Exception
        break
    }
}
Write-Host "Batches completed."