Intune_PaloAlto_UserID

After moving from the AD server to Entra ID, we lost User-ID information. The solutions provided by the Palo Alto team were not acceptable for our environment, so I started developing this script with some help from AI. It’s not perfect, but it serves as a temporary solution for now.
In the future, we may integrate Defender data to improve accuracy.

This Python script retrieves Intune wired IPv4 data from Microsoft Graph API v2 Beta.
It uses a Tenant ID, Client ID, and Client Secret (password), though you can modify it to use a direct token if preferred.
You will also need the Palo Alto Firewall IP (User-ID distributor) and must ensure User-ID is enabled in the interface services.
Select the network subnets you want to query.
Before running, install the required dependencies:
pip install requests msal
Tested on Python 3.10 and 3.12.
