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

#ifndef __DRYADNODEADDRESS_H__
#define __DRYADNODEADDRESS_H__

#pragma once

#pragma prefast(push)
#pragma prefast(disable:24002, "struct sockaddr not ipv6 compatible")

class DrMemoryReader;
class DrMemoryWriter;
class DrLastAccessTable;

extern DrLastAccessTable *g_pDrLastAccessTable;

void DrInitLastAccessTable();

/*
 * Dryad Node Address
 *
 * Used to represent the address of a cosmos node (EN, DRM, etc.) on the network
 */


static const int k_MaxHostNameLength = 255;
static const int k_MaxIpAddressesPerHostName = 16;

class DrNodeAddress
{
public:

    //Default c'tor. Nulls out both the internet address and port
    inline DrNodeAddress();

    inline DrNodeAddress( const char *pszName, DrPortNumber defaultPort )
    {
        Set( pszName, defaultPort );
    }

    //Copy c'tor.
    inline DrNodeAddress(const DrNodeAddress& addr);

    inline DrNodeAddress& operator=(const DrNodeAddress& addr);

    //Create address with specified internet address and port
    inline DrNodeAddress(const IN_ADDR& ina, DrPortNumber wPort);

    // sets using an IP address in network byte order
    inline void Set(const IN_ADDR& ina, DrPortNumber wPort);

    // returns false if not a valid ip/node address
    bool Set(const struct sockaddr *pSockAddr, Size_t addrLen)
    {
        return Set((const struct sockaddr_in *)(const void *)pSockAddr, addrLen);
    }

    // returns false if not a valid ip/node address
    bool Set(const struct sockaddr_in *pSockAddr, Size_t addrLen = sizeof(struct sockaddr_in));

    // sets using an IP address in host byte order
    inline void Set(DrIpAddress ipAddr, DrPortNumber wPort);

    inline void Clear()
    {
        Set(DrInvalidIpAddress, DrInvalidPortNumber);
    }

    inline bool operator==(const DrNodeAddress& other) const
    {
        return (m_ina.S_un.S_addr == other.m_ina.S_un.S_addr && m_wPort == other.m_wPort);
    }

    inline bool operator!=(const DrNodeAddress& other) const
    {
        return (m_ina.S_un.S_addr != other.m_ina.S_un.S_addr || m_wPort != other.m_wPort);
    }

    inline void SetLocal(DrPortNumber wPort)
    {
        Set(DrLocalIpAddress, wPort);
    }

    void GetSockAddr(struct sockaddr_in *pAddr, Size_t len = sizeof(struct sockaddr_in)) const
    {
        LogAssert (len >= sizeof(struct sockaddr_in));
        memset(pAddr, 0, len);
        pAddr->sin_family = AF_INET;
        pAddr->sin_port = htons(m_wPort);
        pAddr->sin_addr = m_ina;
    }
        
    
    // Gets the IP address in host byte order
    inline DrIpAddress GetIpAddress() const
    {
        return ntohl(m_ina.S_un.S_addr);
    }

    // Gets the IP address in network byte order
    inline UInt32 GetIpAddressNetworkOrder() const
    {
        return m_ina.S_un.S_addr;
    }

    // Gets the IP address in network byte order
    const IN_ADDR& GetInAddr() const
    {
        return m_ina;
    }
    
    // Gets the port number in host byte order
    inline DrPortNumber GetPort() const
    {
        return m_wPort;
    }

    // Sets the port number from a host byte ordered value
    inline void SetPort(DrPortNumber port)
    {
        m_wPort = port;
    }

    inline bool IsNull() const
    {
        return m_ina.S_un.S_addr == 0 && m_wPort == 0;
    }

    inline void SetToNull()
    {
        m_ina.S_un.S_addr=0;
        m_wPort=0;
    }
    
    // Looks up a host name using DNS
    // Note that this is a blocking request
    // Returns up to addressBuffLen entries. If there are more entries than this, the list is truncated without error.
    // Returns DrError_HostNotFound if no hosts match the name.
    static DrError LookupHostName(
        const char *pszHostName,
        /* out */ DrIpAddress *pAddressBuff,
        UInt32 addressBuffLen,
        /* out */ UInt32 *pNumReturnedAddresses);
    
    // Parses a name in the form "#.#.#.#:port" or "dns-name:port" and splits out the host name and port.
    // If ":port" is missing, uses the default port.
    // Returns DrError_InvalidParameter if the string is malformed.
    static DrError ParseHostPortName(/* out */ char *pHostNameBuffer, Size_t buffLen, /*out */ DrPortNumber *pPort, const char *pszName, DrPortNumber defaultPort = DrAnyPortNumber, UInt32 *pInstanceNumOut = NULL);

    // Parses a name in the form "#.#.#.#:port" or "dns-name:port" and splits out the host name and port.
    // If ":port" is missing, uses the default port.
    // strOut is replaced with the parsed host name
    // Returns DrError_InvalidParameter if the string is malformed.
    static DrError ParseHostPortName(DrStr& strOut, /*out */ DrPortNumber *pPort, const char *pszName, DrPortNumber defaultPort = DrAnyPortNumber, UInt32 *pInstanceNumOut = NULL);

    // Parses a stringified IP address in the form "#.#.#.#" into a host-order IP address.
    // Returns DrError_InvalidParameter if the string is malformed.
    static DrError ParseIpAddress(const char *pszIpAddress, /* out */ DrIpAddress *pIpAddr);

    // Parses a name in the form "#.#.#.#:port" or "dns-name:port" and resolves it to an address.
    // If ":port" is missing, uses the default port.
    // If there is more than one address associated with a DNS name, uses the first one.
    // Returns DrError_InvalidParameter if the string is malformed.
    // Note that this method may block for DNS resolution if a DNS name is used.
    DrError Set(const char *pszName, DrPortNumber defaultPort=DrAnyPortNumber);

    // Converts the contained IP/port address to a string of the form "#.#.#.#:port". If the contained port number matches defaultPort, the
    // port number is not included in the string.
    // buffSize must be at least 22 or DrError_StringTooLong is returned.
    DrError ToAddressPortString(char *pBuffer, Size_t buffSize, DrPortNumber defaultPort = DrAnyPortNumber) const;
    DrError ToAddressPortString(WCHAR *pBuffer, Size_t buffSize, DrPortNumber defaultPort = DrAnyPortNumber) const;
    DrStr& AppendToString(DrStr& strOut, DrPortNumber defaultPort = DrAnyPortNumber) const;
    //JC DrWStr& AppendToString(DrWStr& strOut, DrPortNumber defaultPort = DrAnyPortNumber) const;

    // Generates a 32-bit hash of the node address
    inline UInt32 Hash() const
    {
        return (UInt32)GetIpAddress() + (UInt32)GetPort();
    }
    
private:
    IN_ADDR m_ina;               // IP address in network byte order
    DrPortNumber m_wPort;   // Port number in host byte order

};    

class DrNodeAddressString : public DrStr32
{
public:
    DrNodeAddressString(const DrNodeAddress& addr)
    {
        addr.AppendToString(*this, DrAnyPortNumber);
    }
};

// This macro can be used to obtain a temporary "const char *" string equivalent for a node address. It can be used
// as the parameter to a method call; the pointer will become invalid after the function returns
#define DRNODEADDRESSSTRING(addr)    (DrNodeAddressString(addr).GetString())


// DrNodeAddressList is a list of node addresses, as you might get from resolving a name. It is more efficient when there is only one name.
class DrNodeAddressList
{
public:
    DrNodeAddressList()
    {
        m_pMultipleAddresses = &m_singleAddress;
        m_numAllocated = 1;
        m_numEntries = 0;
    }

    ~DrNodeAddressList()
    {
        Clear();
    }

    // discards all the entries, but doesn't free memory
    void Discard()
    {
        m_numEntries = 0;
    }

    // Discards all entries and frees allocated memory.
    void Clear()
    {
        if (m_pMultipleAddresses != &m_singleAddress) {
            delete[] m_pMultipleAddresses;
            m_pMultipleAddresses = &m_singleAddress;
            m_numAllocated = 1;
        }
        m_numEntries = 0;
    }

    void GrowTo(UInt32 numAllocated)
    {
        if (numAllocated > m_numAllocated) {
            UInt32 nNew = 2 * m_numAllocated;
            if (nNew < numAllocated) {
                nNew = numAllocated;
            }
            if (nNew < 8) {
                nNew = 8;
            }
            DrNodeAddress *pNew = new DrNodeAddress[nNew];
            if (m_numEntries > 0) {
                memcpy(pNew, m_pMultipleAddresses, m_numEntries * sizeof(DrNodeAddress));
            }
            if (m_pMultipleAddresses != &m_singleAddress) {
                delete[] m_pMultipleAddresses;
            }
            m_pMultipleAddresses = pNew;
            m_numAllocated = nNew;
        }
    }
    
    DrNodeAddress *AddEntry(const DrNodeAddress *pOther = NULL)
    {
        GrowTo(m_numEntries+1);
        DrNodeAddress *pEntry = m_pMultipleAddresses + m_numEntries;
        m_numEntries++;
        
        if (pOther != NULL) {
            (*pEntry) = (*pOther);
        } else {
            pEntry->Clear();
        }

        return pEntry;
    }

    DrNodeAddress& operator[](UInt32 index)
    {
        LogAssert(index < m_numEntries);
        return m_pMultipleAddresses[index];
    }

    const DrNodeAddress& operator[](UInt32 index) const
    {
        LogAssert(index < m_numEntries);
        return m_pMultipleAddresses[index];
    }

    UInt32 GetLength() const
    {
        return m_numEntries;
    }

    // This call may block for DNS
    // It resolves the specified host name (with optional ":port") to a list of IP addresses and *appends* those to this DrNodeAddressList, filling in
    // the port number for each.
    // Note that since this request appends to the existing list, you must Clear() or Discard() the list before you make this
    // call if you want the results to replace the existing set.
    // Returns DrError_HostNotFound if no hosts match the name.
    DrError ResolveHostName(const char *pszHostName, DrPortNumber defaultPort=DrInvalidPortNumber);
    
private:
    DrNodeAddress m_singleAddress;
    DrNodeAddress *m_pMultipleAddresses; // Points to m_singleAddress if there is 1 entry; otherwise, a heap array.
    UInt32 m_numAllocated;
    UInt32 m_numEntries;
};


// After the first failure to send to a host, don't send to it again until Now + this interval
const DrTimeInterval k_initialDelayedSendTimeInterval = DrTimeInterval_Second * 10;

// Don't let the delayed send interval grow beyond this
const DrTimeInterval k_maxDelayedSendInterval = DrTimeInterval_Second * 60; 

// XStream-specific conversions between fabric fault domain and autopilot pod names
//JC DrError DrPodNameToFaultDomain(__in PCSTR pszPodName, __out XsFaultDomain *pFaultDomainOut);

// The return value is an internalized string of the for "pod%u";
//JC PCSTR DrFaultDomainToPodName( __in XsFaultDomain faultDomain);

    

// Manages a host name and a port number
// Also keeps track of a fault domain and an upgrade domain (for use in load balancing/load optimization)
class DrHostAndPort
{
public:
    DrHostAndPort()
    {
        m_pszHostName = NULL;
        m_portNumber = DrInvalidPortNumber;
        m_pszPodName = NULL;
//JC        m_upgradeDomain = 0;
//JC        m_faultDomain = 0;
//JC        m_fValidFaultDomain = true;
        m_fValidPod = true;
    }

    ~DrHostAndPort()
    {
    }

    DrHostAndPort& Set(const DrHostAndPort& other)
    {
        m_pszHostName = other.m_pszHostName;
        m_portNumber = other.m_portNumber;
        m_pszPodName = other.m_pszPodName;
//JC        m_upgradeDomain = other.m_upgradeDomain;
//JC        m_faultDomain = other.m_faultDomain;
//JC        m_fValidFaultDomain = other.m_fValidFaultDomain;
        m_fValidPod = other.m_fValidPod;
        return *this;
    }

    DrHostAndPort(const DrHostAndPort& other)
    {
        Set(other);
    }

    DrHostAndPort& operator=(const DrHostAndPort& other)
    {
        return Set(other);
    }

    // Note, doesn't compare pod and upgrade domain
    bool operator==(const DrHostAndPort &other) const{
        // only need to compare hostname addresses since they are internalized
        return (m_portNumber == other.m_portNumber)  && 
                   (m_pszHostName == other.m_pszHostName);  
    }

/*JC
// Note: this replaces the fault domain with the one encoded in the pod name
    void Set(const char *pszHostName, DrPortNumber port, const char *pszPodName, XsUpgradeDomain upgradeDomain)
    {
        m_pszHostName = g_DrInternalizedStrings.InternalizeStringLowerCase(pszHostName);
        m_portNumber = port;
        m_pszPodName = g_DrInternalizedStrings.InternalizeStringLowerCase(pszPodName);
        m_fValidPod = true;
//JC        ReplaceFaultDomainFromPod();
//JC        m_upgradeDomain = upgradeDomain;
    }

    // Note: this replaces the fault domain with the one encoded in the pod name
    void SetPodName(const char *pszPodName)
    {
        m_pszPodName = g_DrInternalizedStrings.InternalizeStringLowerCase(pszPodName);
        m_fValidPod = true;
        ReplaceFaultDomainFromPod();
    }
*/
    // Sets the pod name without explicitly changing the fault domain.
    void SetPodNameNoFaultDomainUpdate(const char *pszPodName)
    {
        m_pszPodName = g_DrInternalizedStrings.InternalizeStringLowerCase(pszPodName);
        m_fValidPod = true;
    }
/*JC
    // Note: this replaces the pod with "pod%u"
    void SetFaultDomain(XsFaultDomain faultDomain)
    {
        m_faultDomain = faultDomain;
        m_fValidFaultDomain = true;
        ReplacePodFromFaultDomain();
    }

    // Sets the fault domain without explictly changing the pod name.
    void SetFaultDomainNoPodUpdate(XsFaultDomain faultDomain)
    {
        m_faultDomain = faultDomain;
        m_fValidFaultDomain = true;
    }

    void SetUpgradeDomain(XsUpgradeDomain upgradeDomain)
    {
        m_upgradeDomain = upgradeDomain;
    }


    // Note: this replaces the fault domain with the one encoded in the pod name
    DrError SetWithDefaultPort(const char *pszHostName, DrPortNumber defaultPort, const char *pszPodName, XsUpgradeDomain upgradeDomain)
    {
        DrStr64 strHost;
        DrError err = DrNodeAddress::ParseHostPortName(strHost, &m_portNumber, pszHostName, defaultPort);
        if (err == DrError_OK) {
            m_pszHostName = g_DrInternalizedStrings.InternalizeStringLowerCase(strHost.GetString());
            m_pszPodName = g_DrInternalizedStrings.InternalizeStringLowerCase(pszPodName);
            m_fValidPod = true;
            ReplaceFaultDomainFromPod();
            m_upgradeDomain = upgradeDomain;
        }
        return err;
    }
*/

    void SetHostName(const char *pszHostName)
    {
        m_pszHostName = g_DrInternalizedStrings.InternalizeStringLowerCase(pszHostName);
    }

    void SetPort(DrPortNumber port)
    {
        m_portNumber = port;
    }

    const char *GetHostName() const
    {
        return m_pszHostName;
    }

    DrPortNumber GetPort() const
    {
        return m_portNumber;
    }

    // In XStream, the POD name is the fault domain in "pod%u" form
    const char *GetPodName() const
    {
        return m_pszPodName;
    }

/* JC
    // returns false if a POD was set that was not of the form "pod%u".
    bool IsValidFaultDomain() const
    {
        return m_fValidFaultDomain;
    }

    XsFaultDomain GetFaultDomain() const
    {
        return m_faultDomain;
    }

    XsUpgradeDomain GetUpgradeDomain() const
    {
        return m_upgradeDomain;
    }
*/

    void Clear()
    {
        m_pszHostName = NULL;
        m_portNumber = DrInvalidPortNumber;
        // TODO: should pod, faultdomain, upgrade domain, etc. be updated?
    }

    bool IsValid() const
    {
        return m_pszHostName != NULL;
    }

    bool IsInvalid() const
    {
        return m_pszHostName == NULL;
    }
    
    // This call may block for DNS
    // It resolves the host name to a list of IP addresses and *appends* those to the specified DrNodeAddressList, filling in
    // the port number for each.
    // Note that since this request appends to the existing list, you must Clear() or Discard() the list before you make this
    // call if you want the results to replace the existing set.
    // Returns DrError_HostNotFound if no hosts match the name.
    DrError ResolveToAddresses(DrNodeAddressList *pAddresses);

    DrError Unserialize(DrMemoryReader *pReader);
    DrError Serialize(DrMemoryWriter *pWriter) const;
    
    DrStr& AppendToString(DrStr& strOut) const
    {
        strOut.AppendF("%s:%u", m_pszHostName, m_portNumber);
        return strOut;
    }

    DrStr& ToString(DrStr& strOut) const
    {
        strOut.SetToEmptyString();
        return AppendToString(strOut);
    }

/* JC
private:
    // Updates the fault domain from the pod name. Note that if the pod name is not of the form "pod%u", the fault domain
    // will be set to 0, m_fValidFaultDomain will be set to false, and and DRError_InvalidParameter will be returned.
    DrError ReplaceFaultDomainFromPod();

    // Sets the pod name to "pod%u" from the fault domain.
    void ReplacePodFromFaultDomain()
    {
        m_pszPodName = DrFaultDomainToPodName(m_faultDomain);
        m_fValidPod = true;
    }
*/

private:
    const char * m_pszHostName;  // Internalized
    DrPortNumber m_portNumber;
    const char *m_pszPodName; // Internalized. String form of fault domain as "pod%u"
    //JC XsUpgradeDomain m_upgradeDomain; // default = 0
    //JC XsFaultDomain m_faultDomain; // Fault domain (default = 0). NUmeric form of pod;
    //JC bool m_fValidFaultDomain; // Set to false if fault domain was not encountered in Unserialize(). After Unserialize, Always true unless pod is not "pod%u".
    bool m_fValidPod; // Used only during serialize/Unserialize. Set to false if pod was not encountered in Unserialize(). Always true after Unserialize:
};

class DrHostAndPortString : public DrStr32
{
public:
    DrHostAndPortString(const DrHostAndPort& host)
    {
        host.AppendToString(*this);
    }
};

// For each (ip address, port), keeps track of the last time it was accessed
class DrLastAccessEntry
{
public:
    DrLastAccessEntry()
    {
        m_nextHash           = NULL;
        m_nextAttemptAllowed = DrTimeStamp_LongAgo;
        m_delayTime          = 0;
        m_lastError          = DrError_OK;
    }

    UInt32 Hash()
    {
        return m_nodeAddress.Hash();
    }

public:
    DrLastAccessEntry*  m_nextHash;
    DrNodeAddress       m_nodeAddress;

    // If this is not DrTimeStamp_LongAgo, don't send requests until this time
    DrTimeStamp         m_nextAttemptAllowed;

    // Amount of time to delay next protocol request
    DrTimeInterval      m_delayTime;

    // Last error code reported
    DrError             m_lastError;
};


// This table keeps track of when we last accessed a particular IP/port
// It is used for throttling when we are determining the primary DRM
// Currently only DRM service descriptors go into this table, which is why it is small
const UInt32 k_numLastAccessTableBuckets = 100;

class DrLastAccessTable
{
public:
    DrLastAccessTable();

    // Successful send to this node address, reset throttling
    void UpdateSuccess(const DrNodeAddress& nodeAddress);

    // Send failure.
    // Returns true if we were already at the maximum allowed delay value
    bool UpdateFailure(const DrNodeAddress& nodeAddress, DrError error);

    // When can we next send to this node address?
    // If when is DrTimeStamp_LongAgo, it means there is no delay at all
    DrError GetDelay(const DrNodeAddress& nodeAddress, DrTimeStamp* when);

private:
    void Lock()
    {
        m_lock.Enter();
    }

    void Unlock()
    {
        m_lock.Leave();
    }

    // You must have the lock to call these functions
    DrLastAccessEntry*  FindOrCreate(const DrNodeAddress& nodeAddress);
    DrLastAccessEntry*  Find(const DrNodeAddress& nodeAddress);

public:
    DrCriticalSection   m_lock;
    DrLastAccessEntry*  m_head[k_numLastAccessTableBuckets];
};


#define DRHOSTANDPORTSTRING(host)    (DrHostAndPortString(host).GetString())

// Manages a list of host names (with optional port #) strings
class DrHostNameList : public DrPropertyParser{
private:
    const static UInt32 INVALID_PRIMARY_HOST = 0xFFFFFFFF;
public:
    DrHostNameList()
    {
        m_pMultipleHosts = &m_singleHost;
        m_numAllocated = 1;
        m_numEntries = 0;
        m_primary = INVALID_PRIMARY_HOST;
    }

    ~DrHostNameList()
    {
        Clear();
    }

    // Set does a diff with what is already there, so this must be initialized
    DrHostNameList& Set(const DrHostNameList& other, bool forceReordering = false);

    DrHostNameList(const DrHostNameList& other)
    {
        m_pMultipleHosts = &m_singleHost;
        m_numAllocated = 1;
        m_numEntries = 0;
        m_primary = INVALID_PRIMARY_HOST;

        Set(other);
    }

    DrHostNameList& operator=(const DrHostNameList& other)
    {
        // Free previously allocated memory when and only when new name list will not fit
        // into already allocated region
        if ((other.m_numEntries > m_numAllocated) && (m_pMultipleHosts != &m_singleHost))
            Clear();

        return Set(other, true);
    }
    
    void Clear()
    {
        if (m_pMultipleHosts != &m_singleHost) {
            delete[] m_pMultipleHosts;
            m_pMultipleHosts = &m_singleHost;
            m_numAllocated = 1;
        }
        m_numEntries = 0;
        m_primary = INVALID_PRIMARY_HOST;
    }

    void GrowTo(UInt32 numAllocated)
    {
        if (numAllocated > m_numAllocated) {
            UInt32 nNew = 2 * m_numAllocated;
            if (nNew < numAllocated) {
                nNew = numAllocated;
            }
            if (nNew < 8) {
                nNew = 8;
            }
            DrHostAndPort *pNew = new DrHostAndPort[nNew];
            for (UInt32 i = 0; i < m_numEntries; i++) {
                pNew[i] = m_pMultipleHosts[i];
            }
            if (m_pMultipleHosts != &m_singleHost) {
                delete[] m_pMultipleHosts;
            }
            m_pMultipleHosts = pNew;
            m_numAllocated = nNew;
        }
    }
    
    DrHostAndPort *AddEntry()
    {
        GrowTo(m_numEntries+1);
        DrHostAndPort *pEntry = m_pMultipleHosts + m_numEntries;
        m_numEntries++;
        return pEntry;
    }

    const DrHostAndPort& operator[](UInt32 index) const
    {
        LogAssert(index < m_numEntries);
        return m_pMultipleHosts[index];
    }

    DrHostAndPort& operator[](UInt32 index)
    {
        LogAssert(index < m_numEntries);
        return m_pMultipleHosts[index];
    }

    UInt32 GetLength() const
    {
        return m_numEntries;
    }

    // Demote host to bottom of the list
    // If the host was the primary, then invalidate the primary
    void DemoteHost(DrHostAndPort &host){
        DrHostAndPort saveHost;
        saveHost.Set(host);
       
        LogAssert(m_numEntries > 0);

        for(UInt32 i = 0; i < m_numEntries; i ++)
        {
            if(host == m_pMultipleHosts[i])
            {
                if (m_primary == i)
                    m_primary = INVALID_PRIMARY_HOST;      // this host is deemed dead
                
                if (i < m_numEntries - 1)
                {
                    for(; i < m_numEntries - 1; i++)
                    {
                        m_pMultipleHosts[i].Set(m_pMultipleHosts[i + 1]);                   
                    }
                    
                    m_pMultipleHosts[m_numEntries - 1].Set(saveHost);
                }

                break;
            }
        }
    }
    bool IsPrimaryValid(void) const
    {
        return m_primary != INVALID_PRIMARY_HOST;
    }
    
    bool IsPrimaryInvalid(void) const
    {
        return m_primary == INVALID_PRIMARY_HOST;
    }
    
    void SetPrimaryInvalid(void)
    {
        m_primary = INVALID_PRIMARY_HOST;
    }

    void SetPrimary(UInt32 primary)
    {
        m_primary = primary;
    }

    UInt32 GetPrimary(void) const{
        return m_primary;
    }

    DrError Serialize(DrMemoryWriter *pWriter) const;
    virtual DrError OnParseProperty(DrMemoryReader *reader, UInt16 property, UInt32 dataLen, void *cookie);
    
    // This call may block for DNS
    // It resolves the list of host names to a list of IP addresses and *appends* those to the specified DrNodeAddressList, filling in
    // the port number for each.
    // Note that since this request appends to the existing list, you must Clear() or Discard() the list before you make this
    // call if you want the results to replace the existing set.
    // Returns DrError_HostNotFound if no hosts match the name.
    DrError ResolveToAddresses(DrNodeAddressList *pAddresses);    

    // return one IP/port pair in *pAddresses* and return one host name in *host*
    DrError ResolveOneHostToAddresses(DrNodeAddressList *pAddressses, bool wantPrimary, DrHostAndPort &host);

    DrStr& AppendToString(DrStr& strOut) const
    {
        if (m_numEntries == 0) {
            strOut.Append("<NoHosts>");
            
        } else {
            m_pMultipleHosts[0].AppendToString(strOut);
            for (UInt32 i = 1; i < m_numEntries; i++) {
                strOut.Append(';');
                m_pMultipleHosts[i].AppendToString(strOut);
            }
        }
        return strOut;
    }

    DrStr& ToString(DrStr& strOut) const
    {
        strOut.SetToEmptyString();
        return AppendToString(strOut);
    }
    
    
protected:

    void SelectOneHost(DrHostAndPort &host, bool wantPrimary = true);

private:
    DrHostAndPort m_singleHost;
    DrHostAndPort *m_pMultipleHosts; // Points to m_singleHost if there is 1 entry; otherwise, a heap array.  If there is a primary it is always the first entry.
    UInt32 m_numAllocated;
    UInt32 m_numEntries;
    UInt32 m_primary; // This is 0 if there is a primary, and INVALID_PRIMARY_HOST otherwise
};

class DrHostNameListString : public DrStr64
{
public:
    DrHostNameListString(const DrHostNameList& hostList)
    {
        hostList.AppendToString(*this);
    }
};

#define DRHOSTNAMELISTSTRING(hostList)    (DrHostNameListString(hostList).GetString())

/*
 * Inline methods for DrNodeAddress
 */

inline DrNodeAddress::DrNodeAddress()
{
    m_ina.S_un.S_addr=0;
    m_wPort=0;
}

inline DrNodeAddress::DrNodeAddress(const DrNodeAddress& addr)
{
    m_ina=addr.m_ina;
    m_wPort=addr.m_wPort;
}

inline DrNodeAddress& DrNodeAddress::operator=(const DrNodeAddress& addr)
{
    m_ina=addr.m_ina;
    m_wPort=addr.m_wPort;
    return *this;
}

inline DrNodeAddress::DrNodeAddress(const IN_ADDR& ina, DrPortNumber wPort)
{
    m_ina=ina;
    m_wPort=wPort;
}

inline void DrNodeAddress::Set(const IN_ADDR& ina, DrPortNumber wPort)
{
    m_ina=ina;
    m_wPort=wPort;
}

inline void DrNodeAddress::Set(DrIpAddress ipAddr, DrPortNumber wPort)
{
    m_ina.s_addr=htonl(ipAddr);
    m_wPort=wPort;
}

#pragma prefast(pop)

#endif

