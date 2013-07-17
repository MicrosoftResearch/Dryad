/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

*/

#include "DrCommon.h"

#pragma warning (push)
#pragma warning (disable:4365)
#include <ws2tcpip.h>
#pragma warning (pop)

#pragma prefast(disable:24002, "struct sockaddr not ipv6 compatible")

#pragma unmanaged

DrLastAccessTable *g_pDrLastAccessTable;

void DrInitLastAccessTable()
{
    g_pDrLastAccessTable = new DrLastAccessTable();
    LogAssert(g_pDrLastAccessTable != NULL);
}

// returns false if not a valid ip/node address
bool DrNodeAddress::Set(const struct sockaddr_in *pSockAddr, Size_t addrLen)
{
    if (pSockAddr == NULL) {
        return false;
    }

    if (addrLen < sizeof(struct sockaddr_in)) {
        return false;
    }

    if (pSockAddr->sin_family != AF_INET) {
        return false;
    }

    Set(pSockAddr->sin_addr, ntohs(pSockAddr->sin_port));

    return true;
}

// Looks up a host name using DNS
// Note that this is a blocking request
// Returns up to addressBuffLen entries. If there are more entries than this, the list is truncated without error.
DrError DrNodeAddress::LookupHostName(
    const char *pszHostName,
    /* out */ DrIpAddress *pAddressBuff,
    UInt32 addressBuffLen,
    /* out */ UInt32 *pNumReturnedAddresses)
{
    struct addrinfo hints;
    struct addrinfo *pResults = NULL;
    memset(&hints, 0, sizeof(hints));

    *pNumReturnedAddresses = 0;
    
    hints.ai_family = PF_INET;
    int ret = EAI_AGAIN;

    while (ret == EAI_AGAIN) {
        ret = getaddrinfo(pszHostName, NULL, &hints, &pResults);

        if (ret == 0 && pResults == NULL) {
            ret = EAI_NODATA;
        }
    }
    
    if (ret != 0) {
        goto done;
    }

    UInt32 n = 0;
    struct addrinfo *pResultEntry = pResults;
    while (pResultEntry != NULL && addressBuffLen > 0) {
        if (pResultEntry->ai_family == PF_INET && pResultEntry->ai_addr != NULL && pResultEntry->ai_addrlen == sizeof(sockaddr_in)) {
            const struct sockaddr_in *paddr = (const struct sockaddr_in *)(const void *)(pResultEntry->ai_addr);
            DrIpAddress addr = ntohl(paddr->sin_addr.S_un.S_addr);
            *pAddressBuff = addr;
            pAddressBuff++;
            addressBuffLen--;
            n++;
        }
            pResultEntry = pResultEntry->ai_next;
    }

    if (n == 0) {
        ret = EAI_NODATA;
    }

    *pNumReturnedAddresses = n;
    ret = 0;

done:
    if (pResults != NULL) {
        freeaddrinfo(pResults);
    }

    switch(ret) {
        case EAI_NODATA:
            ret = DrError_HostNotFound;
            break;
    }
    
    return ret;
}


// Converts the contained IP/port address to a string of the form "#.#.#.#:port". If the contained port number matches defaultPort, the
// port number is not included in the string.
DrStr& DrNodeAddress::AppendToString(DrStr& strOut, DrPortNumber defaultPort) const
{
    if (m_wPort == defaultPort) {
        strOut.AppendF("%u.%u.%u.%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4);
    } else {
        strOut.AppendF("%u.%u.%u.%u:%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4,
            (DrIpAddress ) m_wPort);
    }
    
    return strOut;
}
/* JC
// Converts the contained IP/port address to a string of the form "#.#.#.#:port". If the contained port number matches defaultPort, the
// port number is not included in the string.
DrWStr& DrNodeAddress::AppendToString(DrWStr& strOut, DrPortNumber defaultPort) const
{
    if (m_wPort == defaultPort) {
        strOut.AppendF(L"%u.%u.%u.%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4);
    } else {
        strOut.AppendF(L"%u.%u.%u.%u:%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4,
            (DrIpAddress ) m_wPort);
    }
    
    return strOut;
}
*/

// Converts the contained IP/port address to a string of the form "#.#.#.#:port". If the contained port number matches defaultPort, the
// port number is not included in the string.
// buffSize must be at least 22 or DrError_StringTooLong is returned.
DrError DrNodeAddress::ToAddressPortString(char *pBuffer, Size_t buffSize, DrPortNumber defaultPort) const
{
    HRESULT hr;
    if (m_wPort == defaultPort) {
        hr =StringCbPrintfA(pBuffer, buffSize,  "%u.%u.%u.%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4);
    } else {
        hr =StringCbPrintfA(pBuffer, buffSize,  "%u.%u.%u.%u:%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4,
            (DrIpAddress ) m_wPort);
    }
    
    return SUCCEEDED(hr) ? DrError_OK : DrError_StringTooLong;
}

// Converts the contained IP/port address to a string of the form "#.#.#.#:port". If the contained port number matches defaultPort, the
// port number is not included in the string.
// buffSize must be at least 22 or DrError_StringTooLong is returned.
DrError DrNodeAddress::ToAddressPortString(WCHAR *pBuffer, Size_t buffSize, DrPortNumber defaultPort) const
{
    HRESULT hr;
    if (m_wPort == defaultPort) {
        hr =StringCbPrintfW(pBuffer, buffSize,  L"%u.%u.%u.%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4);
    } else {
        hr =StringCbPrintfW(pBuffer, buffSize,  L"%u.%u.%u.%u:%u",
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b1,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b2,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b3,
            (DrIpAddress ) m_ina.S_un.S_un_b.s_b4,
            (DrIpAddress ) m_wPort);
    }
    
    return SUCCEEDED(hr) ? DrError_OK : DrError_StringTooLong;
}


static DrError InternalParseHostPortName(
    Size_t hostLength,
    /*out */ DrPortNumber *pPort,
    const char *pszName,
    DrPortNumber defaultPort,
    UInt32 *pInstanceNumOut)
{
    LogAssert(pPort != NULL);
    
    if (hostLength == 0) {
        return DrError_InvalidParameter;
    }

    Size_t instanceLength = hostLength;
    UInt32 uInstance = 0;
    if (pszName[hostLength] == '!') {
        char c;
        for (instanceLength = hostLength + 1; (c = pszName[instanceLength]) != '\0' && c != ':'; instanceLength++) {
            if (c >= '0' && c <= '9') {
                uInstance = (10 * uInstance) + (UInt32)(c - '0');
                if (uInstance > 65535) {
                    return DrError_InvalidParameter;
                }
            } else {
                return DrError_InvalidParameter;
            }
        }
    }

    DrPortNumber finalPort;
    
    if (pszName[instanceLength] == ':') {
        const char *pszPort = pszName + instanceLength + 1;
        DrError err = DrStringToPortNumber(pszPort, &finalPort);
        if (err != DrError_OK) {
            return err;
        }
    } else {
        finalPort = defaultPort;
    }

    if (uInstance != 0 && finalPort != DrInvalidPortNumber && finalPort != DrAnyPortNumber) {
        finalPort = (DrPortNumber)(finalPort + uInstance);
    }
    
   *pPort = finalPort;
   if (pInstanceNumOut != NULL) {
    *pInstanceNumOut = uInstance;
   }
    return DrError_OK;
}

inline static Size_t InternalHostLength(const char *pszName)
{
    Size_t hostLength;

    LogAssert(pszName != NULL);
    
    for (hostLength = 0; pszName[hostLength] != '\0' && pszName[hostLength] != ':' && pszName[hostLength] != '!'; hostLength++) {
        // Just counting
    }

    return hostLength;
}

// Parses a name in the forms:
//            "#.#.#.#"
//            "dns-name"
//            "#.#.#.#!instance-num"
//            "dns-name!instance-num"
//            "#.#.#.#:port"
//            "dns-name:port"
//            "#.#.#.#!instance-num:port"
//            "dns-name!instance-num:port"
// and splits out the host name and port.
// If ":port" is missing, uses the default port.
// if instance-num is present, it is added to the final port number.
// Returns DrError_InvalidParameter if the string is malformed.
DrError DrNodeAddress::ParseHostPortName(
    /* out */ char *pHostNameBuffer,
    Size_t buffLen,
    /*out */ DrPortNumber *pPort,
    const char *pszName,
    DrPortNumber defaultPort,
    UInt32 *pInstanceNumOut)
{
    LogAssert(pHostNameBuffer != NULL);
    Size_t hostLength = InternalHostLength(pszName);
    
    if (hostLength == 0 || buffLen < hostLength+1) {
        return DrError_InvalidParameter;
    }

    memcpy(pHostNameBuffer, pszName, hostLength);
    pHostNameBuffer[hostLength] = '\0';

    return InternalParseHostPortName(
        hostLength,
        pPort,
        pszName,
        defaultPort,
        pInstanceNumOut);
}

// Parses a name in the form "#.#.#.#:port" or "dns-name:port" and splits out the host name and port.
// If ":port" is missing, uses the default port.
// strOut is replaced with the parsed host name
// Returns DrError_InvalidParameter if the string is malformed.
DrError DrNodeAddress::ParseHostPortName(DrStr& strOut, /*out */ DrPortNumber *pPort, const char *pszName, DrPortNumber defaultPort, UInt32 *pInstanceNumOut)
{
    Size_t hostLength = InternalHostLength(pszName);
    
    if (hostLength == 0) {
        return DrError_InvalidParameter;
    }

    strOut.Set(pszName, hostLength);

    return InternalParseHostPortName(
        hostLength,
        pPort,
        pszName,
        defaultPort,
        pInstanceNumOut);
}

// Parses a stringified IP address in the form "#.#.#.#" into a host-order IP address.
// Returns DrError_InvalidParameter if the string is malformed.
DrError DrNodeAddress::ParseIpAddress(const char *pszIpAddress, /* out */ DrIpAddress *pIpAddr)
{
    DrIpAddress ipAddr = 0;
    for (int i = 0; i < 4; i++) {
        if (i > 0) {
            ipAddr = ipAddr << 8;
            if (*pszIpAddress != '.') {
                return DrError_InvalidParameter;
            }
            pszIpAddress++;
        }
        if (*pszIpAddress < '0' || *pszIpAddress > '9') {
            return DrError_InvalidParameter;
        }
        UInt32 uByte = 0;
        for (int j = 0; j < 3; j++) {
            char c = *pszIpAddress;
            if (c < '0' || c > '9') {
                break;
            }
            uByte = 10 * uByte + (UInt32)(c - '0');
            if (uByte > 255) {
                return DrError_InvalidParameter;
            }
            pszIpAddress ++;
        }
        ipAddr = ipAddr | uByte;
    }
    if (*pszIpAddress != '\0') {
        return DrError_InvalidParameter;
    }

    *pIpAddr = ipAddr;
    return DrError_OK;
}

// Parses a name in the form "#.#.#.#:port" or "dns-name:port" and resolves it to an address.
// If ":port" is missing, uses the default port.
// If there is more than one address associated with a DNS name, uses the first one.
// Returns DrError_InvalidParameter if the string is malformed.
// Note that this method may block for DNS resolution if a DNS name is used.
DrError DrNodeAddress::Set(const char *pszName, DrPortNumber defaultPort)
{
    char buff[k_MaxHostNameLength+1];
    DrPortNumber port;
    DrIpAddress ipAddr;

    DrError err = ParseHostPortName(buff, sizeof(buff), &port, pszName, defaultPort);
    if (err != DrError_OK) {
        return err;
    }

    // first try to parse it as a numeric IP address
    err = ParseIpAddress(buff, &ipAddr);
    if (err == DrError_OK) {
        Set(ipAddr, port);
    } else {
        UInt32 nRet = 0;
        err = LookupHostName(buff, &ipAddr, 1, &nRet);
        if (err != DrError_OK) {
            return err;
        }
        LogAssert(nRet == 1);
        Set(ipAddr, port);
    }

    return DrError_OK;
}

// This call may block for DNS
// It resolves the specified host name (with optional ":port") to a list of IP addresses and *appends* those to this DrNodeAddressList, filling in
// the port number for each.
// Note that since this request appends to the existing list, you must Clear() or Discard() the list before you make this
// call if you want the results to replace the existing set.
// Returns DrError_HostNotFound if no hosts match the name.
DrError DrNodeAddressList::ResolveHostName(const char *pszHostName, DrPortNumber defaultPort)
{
    char buff[k_MaxHostNameLength+1];
    DrPortNumber port;
    DrIpAddress ipAddrs[32];
    UInt32 nResults = 0;

    DrError err = DrNodeAddress::ParseHostPortName(buff, sizeof(buff), &port, pszHostName, defaultPort);
    if (err != DrError_OK) {
        return err;
    }

    // first try to parse it as a numeric IP address
    err = DrNodeAddress::ParseIpAddress(buff, &(ipAddrs[0]));
    if (err == DrError_OK) {
        nResults = 1;
    } else {
        err = DrNodeAddress::LookupHostName(buff, ipAddrs, 32, &nResults);
        if (err != DrError_OK) {
            return err;
        }
        LogAssert(nResults > 0);
    }

    GrowTo(nResults);
    
    for (UInt32 i = 0; i < nResults; i++) {
        DrNodeAddress a;
        a.Set(ipAddrs[i], port);
        AddEntry(&a);
    }
    
    return DrError_OK;
}

// This call may block for DNS
// It resolves the host name to a list of IP addresses and *appends* those to the specified DrNodeAddressList, filling in
// the port number for each.
// Note that since this request appends to the existing list, you must Clear() or Discard() the list before you make this
// call if you want the results to replace the existing set.
// Returns DrError_HostNotFound if no hosts match the name.
DrError DrHostAndPort::ResolveToAddresses(DrNodeAddressList *pAddresses)
{
    DrError err = pAddresses->ResolveHostName(m_pszHostName, m_portNumber);
    return err;
}

/* JC
DrError DrPodNameToFaultDomain(__in PCSTR pszPodName, __out XsFaultDomain *pFaultDomainOut)
{
    DrError err = DrError_OK;
    *pFaultDomainOut = 0;
    if (pszPodName == NULL || pszPodName[0] == '\0') {
        // just return 0
    } else if (_strnicmp(pszPodName, "pod", 3) == 0) {
        err = DrStringToUInt16(pszPodName+3, pFaultDomainOut);
    } else {
        *pFaultDomainOut = 0;
        err = DrError_InvalidParameter;
    }
    return err;
}

PCSTR DrFaultDomainToPodName( __in XsFaultDomain faultDomain)
{
    DrStr32 strPod("pod");
    strPod.AppendUInt32((UInt32)faultDomain);
    return g_DrInternalizedStrings.InternalizeString(strPod);
}

DrError DrHostAndPort::ReplaceFaultDomainFromPod()
{
    m_faultDomain = 0;
    DrError err = DrPodNameToFaultDomain(m_pszPodName, &m_faultDomain);
    m_fValidFaultDomain = (err == DrError_OK);
    return err;
}
*/

class DrHostAndPortParser : public DrPropertyParser
{
public:
    DrHostAndPortParser(DrHostAndPort *pEntry)
    {
        m_pEntry = pEntry;
    }

    virtual ~DrHostAndPortParser()
    {
    }
    
    virtual DrError OnParseProperty(DrMemoryReader *reader, UInt16 enumID, UInt32 dataLen, void *cookie)
    {
        if (reader->GetStatus() == DrError_OK) {
            switch(enumID) {
                case Prop_Dryad_Port:
                    {
                        UInt16 port;
                        
                        if (reader->ReadNextUInt16Property(Prop_Dryad_Port, &port) == DrError_OK) {
                            m_pEntry->SetPort(port);
                        }
                    }
                    break;

                case Prop_Dryad_ShortHostName:
                case Prop_Dryad_LongHostName:
                    {
                        const char *pszHost;
                        if (reader->ReadNextStringProperty(enumID, &pszHost) == DrError_OK) {
                            m_pEntry->SetHostName(pszHost);
                        }
                    }
                    break;

/*                case Prop_Dryad_UpgradeDomain:
                    {
                        UInt16 upgradeDomain;
                        
                        if (reader->ReadNextUInt16Property(Prop_Dryad_UpgradeDomain, &upgradeDomain) == DrError_OK) {
                            m_pEntry->SetUpgradeDomain(upgradeDomain);
                        }
                    }
                    break;
*/
                // NOTE: current code serializes Prop_Dryad_PodName. This will eventually be deprecated in favor
                // of Prop_Dryad_FaultDomain. Here we accept either in preparation for future deprecation.
                case Prop_Dryad_PodName:
                    {
                        const char *pszPodName;
                        if (reader->ReadNextStringProperty(Prop_Dryad_PodName, &pszPodName) == DrError_OK) {
                            m_pEntry->SetPodNameNoFaultDomainUpdate(pszPodName);
                        }
                    }
                    break;

/*JC
                // NOTE: old versions don't serialize this way, but eventually in XStore we will switch to this after all unserializers have been updated.
                // To allow for that while maintaining compatibility with the current API, we will reencode the fault domain into pod name form if
                // the pod is not provided.
                case Prop_Dryad_FaultDomain:
                    {
                        UInt16 uFaultDomain = 0;
                        DrError err = reader->ReadNextUInt16Property(Prop_Dryad_FaultDomain, &uFaultDomain);
                        if (err == DrError_OK) {
                            m_pEntry->SetFaultDomainNoPodUpdate(uFaultDomain);
                        }
                    }
                    break;
*/

                default:
                    reader->SkipNextPropertyOrAggregate();
                    break;
            }

        }
        
        return reader->GetStatus();
    }

private:
    DrHostAndPort *m_pEntry;
};

DrError DrHostAndPort::Unserialize(DrMemoryReader *pReader)
{
//JC    // We keep track of whether pod and/or fault domain were explicitly provided.
    // We keep track of whether pod was explicitly provided.
    
//JC    m_fValidFaultDomain = false;
    m_fValidPod = false;
//JC    m_faultDomain = 0;
    m_pszPodName = NULL;

    DrHostAndPortParser p(this);
    DrError err = pReader->ReadAggregate(DrTag_DrHostAndPort, &p, NULL);

/*JC
    // If fault domain was provided, but pod was not, synthesize the pod. Otherwise, if fault domain
    // was not provided, synthesize it.
    if (m_fValidFaultDomain) {
        if (m_fValidPod) {
            ReplacePodFromFaultDomain();
        }
    } else {
        ReplaceFaultDomainFromPod();
    }
    m_fValidPod = true;
*/

    return err;
}

DrError DrHostAndPort::Serialize(DrMemoryWriter *pWriter) const
{
    pWriter->WriteUInt16Property(Prop_Dryad_BeginTag, DrTag_DrHostAndPort);

    // BUGBUG: should have been LONGATOM. We serialize a long version if length >= 255
    {

        size_t length = 0;
        if (m_pszHostName != NULL) {
            length = strlen(m_pszHostName);
        }
        if (length >= (size_t)_UI8_MAX) {
            // BUGBUG: breaks compat for long host names, but no worse than before
            pWriter->WriteLongStringPropertyWithLength(Prop_Dryad_LongHostName, m_pszHostName, length);
        } else {
            pWriter->WriteShortStringPropertyWithLength(Prop_Dryad_ShortHostName, m_pszHostName, length);
        }
    }
    
    pWriter->WriteUInt16Property(Prop_Dryad_Port, m_portNumber);
/*JC    if (m_fValidFaultDomain) {
        pWriter->WriteUInt16Property(Prop_Dryad_FaultDomain, m_faultDomain);
    }
*/
    if (m_fValidPod && m_pszPodName != NULL) {
        // Note: Prop_Dryad_PodName will eventually be deprecated in favor of Prop_Dryad_FaultDomain. We will continue to serialize
        // this way until all unserializers have been updated.
        pWriter->WriteLongStringProperty(Prop_Dryad_PodName, m_pszPodName);
    }
//JC    pWriter->WriteUInt16Property(Prop_Dryad_UpgradeDomain, m_upgradeDomain);
    
    pWriter->WriteUInt16Property(Prop_Dryad_EndTag, DrTag_DrHostAndPort);
    
    return pWriter->GetStatus();
}

// If this is an update, preserve the ordering of everything already in the list except for the primary, which must be at the top
// If forceReordering is true, overwrite the existing ordering with the new one (but primary must be first).  This is called
// when we DemoteHost() and are putting the new table back
DrHostNameList& DrHostNameList::Set(const DrHostNameList& other, bool forceReordering)
{
    // Sice strings are internalized, we can just copy the pointers
    GrowTo(other.m_numEntries);
    if (other.m_numEntries > 0)
    {
        if (m_numEntries == 0 || forceReordering)
        {
            // There wasn't anything in the existing list, or we want to force reordering, so do a direct copy
            for (UInt32 i = 0; i < other.m_numEntries; i++)
            {
                m_pMultipleHosts[i] = other.m_pMultipleHosts[i];
            }

            if ((other.m_primary >= other.m_numEntries) || (other.m_primary == INVALID_PRIMARY_HOST))
            {
                // Invalid primary, ignore
                m_primary = INVALID_PRIMARY_HOST;
            }
            else
            {
                if (other.m_primary != 0)
                {
                    // Move primary to top
                    DrHostAndPort temp = m_pMultipleHosts[0];
                    m_pMultipleHosts[0] = m_pMultipleHosts[other.m_primary];
                    m_pMultipleHosts[other.m_primary] = temp;
                }

                m_primary = 0;
            }
        }
        else
        {
            // We're updating on top of something we already have
            // Ensure that order is preserved, except that if the primary changes, it is always at the top

            // We will change this below when we encounter the primary
            // Right now we don't know what index this will be in the new list
            m_primary = INVALID_PRIMARY_HOST;

            // Remove hosts which are not exists in new list
            for (UInt32 i = 0; i < m_numEntries; ++i)
            {
                UInt32 j;

                // Does the entry available in both local and new lists?
                for (j = 0; j < other.m_numEntries; ++j)
                {
                  if (m_pMultipleHosts[i] == other.m_pMultipleHosts[j])
                    break;
                }

                // If host is not found in new list, remove it locally
                if (j == other.m_numEntries)
                {
                  --m_numEntries;

                  for (j = i; j < m_numEntries; ++j)
                    m_pMultipleHosts[j] = m_pMultipleHosts[j+1];

                  i--;
                }
            }

            // New entries go at the end
            UInt32 newEntries = m_numEntries;

            // Add new hosts
            for (UInt32 i = 0; i < other.m_numEntries; i++)
            {
                UInt32 j;

                // Does the entry already exist?
                for (j = 0; j < m_numEntries; j++)
                {
                    if (m_pMultipleHosts[j] == other.m_pMultipleHosts[i])
                        break;
                }

                if (j < m_numEntries)
                {
                    // Found - update fields that aren't checked by the == operator above (e.g. upgrade domain)
                    m_pMultipleHosts[j] = other.m_pMultipleHosts[i];

                    if (other.m_primary == i)
                        m_primary = j;
                }
                else
                {
                    LogAssert(newEntries < m_numAllocated);

                    // New entry, append to end of list
                    m_pMultipleHosts[newEntries] = other.m_pMultipleHosts[i];

                    // Convert primary from old index to new index
                    if (other.m_primary == i)
                        m_primary = newEntries;

                    newEntries++;
                }
            }

            // Now move primary to top of list
            if ((m_primary != INVALID_PRIMARY_HOST) && (m_primary != 0))
            {
                DrHostAndPort temp = m_pMultipleHosts[0];
                m_pMultipleHosts[0] = m_pMultipleHosts[m_primary];
                m_pMultipleHosts[m_primary] = temp;

                m_primary = 0;
            }
        }
    }

    m_numEntries = other.m_numEntries;
    return *this;
}

DrError DrHostNameList::Serialize(DrMemoryWriter *pWriter) const{        
    pWriter->WriteUInt16Property(Prop_Dryad_BeginTag, DrTag_DrHostNameList);
    
    pWriter->WriteUInt32Property(Prop_Dryad_NumEntries, m_numEntries);
    for (UInt32 i = 0; i < m_numEntries; i++) {
        m_pMultipleHosts[i].Serialize(pWriter);
    }
    pWriter->WriteUInt32Property(Prop_Dryad_PrimaryHost, m_primary);
    //pWriter->WriteUInt32Property(Prop_Dryad_NextHost, m_nextHost);

    pWriter->WriteUInt16Property(Prop_Dryad_EndTag, DrTag_DrHostNameList);

    return pWriter->GetStatus();
}
DrError DrHostNameList::OnParseProperty(DrMemoryReader *reader, UInt16 property, UInt32 dataLen, void *cookie){
      if (reader->GetStatus() == DrError_OK) {
            switch(property) {
                case Prop_Dryad_BeginTag:
                    {
                        UInt16 tagId;
                        if (reader->PeekNextUInt16Property(Prop_Dryad_BeginTag, &tagId) == DrError_OK) {
                            switch(tagId) {
                                case DrTag_DrHostAndPort:
                                    {
                                        DrHostAndPort *pHost = AddEntry();
                                        pHost->Unserialize(reader);
                                    }
                                    break;
                                
                                default:
                                    reader->SetStatus(DrError_InvalidProperty);
                                    break;
                            }
                        }
                    }
                    break;
                case Prop_Dryad_NumEntries:
                    {
                        UInt32 numEntries;                        
                        if (reader->ReadNextUInt32Property(Prop_Dryad_NumEntries, &numEntries) == DrError_OK) {
                            GrowTo(numEntries);
                        }
                    }
                    break;
                case Prop_Dryad_PrimaryHost:
                    {
                        UInt32 primary;
                        if(reader->ReadNextUInt32Property(Prop_Dryad_PrimaryHost, &primary) == DrError_OK){
                            SetPrimary(primary);
                        }
                    }
                    break;
                case Prop_Dryad_NextHost:
                    {
                        UInt32 nextHost;
                        if(reader->ReadNextUInt32Property(Prop_Dryad_NextHost, &nextHost) == DrError_OK){
                            //SetNextHost(nextHost);
                        }
                    }
                    break;
                default:
                    reader->SkipNextPropertyOrAggregate();
                    break;                
            }

        }
        return reader->GetStatus();
}


void DrHostNameList::SelectOneHost(DrHostAndPort &host, bool wantPrimary)
{
    // The primary (or next node to try) is always the first entry in the list
    host.Set(m_pMultipleHosts[0]);
}


// This call may block for DNS
// It resolves the list of host names to a list of IP addresses and *appends* those to the specified DrNodeAddressList, filling in
// the port number for each.
// Note that since this request appends to the existing list, you must Clear() or Discard() the list before you make this
// call if you want the results to replace the existing set.
// Returns DrError_HostNotFound if no hosts match the name.
DrError DrHostNameList::ResolveToAddresses(DrNodeAddressList *pAddresses)
{
    bool errSet = false;
    DrError err = DrError_HostNotFound;
    for (UInt32 i = 0; i < m_numEntries; i++) {
        DrError err2 = m_pMultipleHosts[i].ResolveToAddresses(pAddresses);
        // we succeed this call if at least one host was resolved successfully. Otherwise, we fail with the first
        // error returned.
        if (!errSet || err2 == DrError_OK) {
            err = err2;
            errSet = true;
        }
    }

    return err;
}

DrError DrHostNameList::ResolveOneHostToAddresses(DrNodeAddressList *pAddressses, bool wantPrimary, DrHostAndPort &host){
    SelectOneHost(host, wantPrimary);
    return host.ResolveToAddresses(pAddressses);
}


DrLastAccessTable::DrLastAccessTable()
{
    memset(m_head, 0, sizeof(m_head));
}

DrLastAccessEntry* DrLastAccessTable::FindOrCreate(const DrNodeAddress& nodeAddress)
{
    DrLastAccessEntry *entry = Find(nodeAddress);
    if (entry != NULL)
        return entry;

    UInt32 bucket = nodeAddress.Hash() % k_numLastAccessTableBuckets;
    entry = new DrLastAccessEntry();
    entry->m_nodeAddress = nodeAddress;
    entry->m_nextHash = m_head[bucket];
    m_head[bucket] = entry;
    return entry;
}

DrLastAccessEntry* DrLastAccessTable::Find(const DrNodeAddress& nodeAddress)
{
    UInt32 bucket = nodeAddress.Hash() % k_numLastAccessTableBuckets;

    for (DrLastAccessEntry* search = m_head[bucket]; search != NULL; search = search->m_nextHash)
    {
        if (search->m_nodeAddress == nodeAddress)
            return search;
    }

    return NULL;
}

void DrLastAccessTable::UpdateSuccess(const DrNodeAddress& nodeAddress)
{
    Lock();

    DrLastAccessEntry* entry = FindOrCreate(nodeAddress);

    entry->m_nextAttemptAllowed = DrTimeStamp_LongAgo;
    entry->m_delayTime          = 0;
    entry->m_lastError          = DrError_OK;

    Unlock();
}

// Send failure.
// Returns true if we were already at the maximum allowed delay value
bool DrLastAccessTable::UpdateFailure(const DrNodeAddress& nodeAddress, DrError error)
{
    Lock();

    DrLastAccessEntry* entry = FindOrCreate(nodeAddress);
    bool               wasMax;

    wasMax = (entry->m_delayTime >= k_maxDelayedSendInterval);

    if (!wasMax && (error == DrError_TalkToPrimaryServer))
    {
        entry->m_delayTime = entry->m_delayTime*2 + k_initialDelayedSendTimeInterval;
        if (entry->m_delayTime > k_maxDelayedSendInterval)
            entry->m_delayTime = k_maxDelayedSendInterval;
    }

    entry->m_nextAttemptAllowed = DrGetCurrentTimeStamp() + entry->m_delayTime;
    entry->m_lastError = error;

    Unlock();
    return wasMax;
}

// When can we send to this node address?
// *when will be DrTimeStamp_LongAgo if there is no delay associated
DrError DrLastAccessTable::GetDelay(const DrNodeAddress& nodeAddress, DrTimeStamp* when)
{
    DrError error;
    Lock();

    DrLastAccessEntry* entry = Find(nodeAddress);
    if (entry == NULL)
    {
        *when = DrTimeStamp_LongAgo;
        error = DrError_OK;
    }
    else
    {
        *when = entry->m_nextAttemptAllowed;
        error = entry->m_lastError;
    }

    Unlock();

    return error;
}
