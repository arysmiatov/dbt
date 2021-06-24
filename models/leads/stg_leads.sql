{{ config ( 
    materialized="view" 
)}}

with leads as (

    select
        l.id,
        l.ClientId,
        l.Project,
        l.Developer,
        l.UtmMedium,
        l.UtmSource,
        CASE
            when l.Source=1 then 'Site App'
            when l.Source=2 then 'Calls'
            when l.Source=4 then 'InstagramComments'
            when l.Source=8 then 'FacebookComments'
            when l.Source=16    then 'Facebook'
            when l.Source=32    then 'Cross: PreVerification'
            when l.Source=64    then 'Cross: PostVerification'
            when l.Source=128   then 'Cross: Survey'
            when l.Source=256   then 'Cross: Email'
            when l.Source=512   then 'Cross: Call'  
            when l.Source=1024  then 'Cross: Chat'		     
            when l.Source=2048  then 'External'
            when l.Source=4096  then 'DeveloperChat'
            when l.Source=8192  then 'AppVerification'
            when l.Source=16384  then 'CallVerification'
            when l.Source=32768  then 'ColdCall (External)'
            when l.Source=65536 then 'Messengers'
            when l.Source=524288 then 'InstagramPage'
            when l.Source=131072 then 'ClientCardV2Rec'
            when l.Source=262144 then 'ColdCall HMS'
            when l.Source=1073741824 then 'Other'
        end as LeadSource,
        CASE
            when l.VerificationStatus = 0 then 'None'
            when l.VerificationStatus = 2 then 'Verified'
            when l.VerificationStatus = 4 then 'NotVerified'
            when l.VerificationStatus = 8 then 'Verified'
            when l.VerificationStatus = 16 then 'NotVerified'
        end as VerificationStatus,
        CASE
            when l.ProjectStatus=1 then 'Trial'
            when l.ProjectStatus=2 then 'Free'
            when l.ProjectStatus=4 then 'Premium'
            when l.ProjectStatus=16 then 'Basic'
            when l.ProjectStatus=32 then 'Start'
            when l.ProjectStatus=64 then 'Transaction'
        end as ProjectStatus,
        date_add(l.date, interval 6 HOUR) as date,

    from `homsters-kz-dwh.raw_data.leads` l
    where date(l.date) >= '2021-06-01'
)

select
    *
from leads 