using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Daedalus.Logging;

namespace Daedalus.Networking.UDP;

/// <summary>
/// Provides centralized UDP listening functionality with support for multiple clients and subscribers.
/// Manages UDP connections, buffers incoming data, and notifies subscribers through callbacks.
/// IPv4 only - IPv6 is not supported. This is because the hashing function for differentating
/// subscribers with different ID's and endpoints is encoded in a 64-bit integer (ulong)
/// Example usage:
///
/// ```c#
/// public static void Listener1(CancellationToken token)
/// {
///
///     Action<ulong> onUdpDatagramReceived = (ulong subKey) =>
///     {
///         if (token.IsCancellationRequested)
///         {
///             DaedalusUdpListener.DeregisterUdpClient(subKey);
///             return;
///         }
///         var dat = DaedalusUdpListener.Receive(subKey);
///         if (dat == null || dat.Buffer == null) return;
///         Console.WriteLine($"From Listener 1: {System.Text.Encoding.UTF8.GetString(dat.Buffer)}");
///     };
///
///     IPEndPoint ep = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 14550);
///     DaedalusUdpListener.RegisterUdpClient(ep, onUdpDatagramReceived);
/// }
/// ```
///
/// </summary>
public static class DaedalusUdpListener
{
    private const uint MAX_BUFFER_SIZE = 512;

    private static SortedSet<ushort> _subIdsInuse
        = new SortedSet<ushort>();

    private static HashSet<ushort> _freedSubIds
        = new HashSet<ushort>();

    // IP:Port encoded in ulong -> UdpClient
    private static ConcurrentDictionary<ulong, UdpClient> _udpClients
        = new ConcurrentDictionary<ulong, UdpClient>();

    // IP:Port (representing UdpClient) -> Task (representing ListenToClient() loop)
    private static ConcurrentDictionary<ulong, Task> _udpListeningTasks
        = new ConcurrentDictionary<ulong, Task>();

    // IP:Port:SubID encoded in ulong -> Buffer
    private static ConcurrentDictionary<ulong, ConcurrentQueue<UdpReceiveResult>> _subscriberBuffers
        = new ConcurrentDictionary<ulong, ConcurrentQueue<UdpReceiveResult>>();

    // IP:Port:SubID encoded in ulong -> Callback
    private static ConcurrentDictionary<ulong, Action<ulong>> _subCallbacks
        = new ConcurrentDictionary<ulong, Action<ulong>>();

    private static CancellationTokenSource _cancellationTokenSource;

    private static object _registrationLock = new object();

    static DaedalusUdpListener()
    {
        AppDomain.CurrentDomain.ProcessExit += (_, __) => { Dispose(); };
        _cancellationTokenSource = new CancellationTokenSource();
    }

    /// <summary>
    /// Registers a new UDP client for the specified endpoint with a callback.
    /// </summary>
    /// <param name="ipEndpoint">The IP endpoint to listen on (IPv4 only)</param>
    /// <param name="callback">Action to invoke when data is received</param>
    /// <returns>A unique subscriber key identifying this registration, or 0 if IPv6 is passed in</returns>
    public static ulong RegisterUdpClient(IPEndPoint ipEndpoint, Action<ulong> callback)
    {
        lock (_registrationLock)
        {
            if (ipEndpoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                Console.WriteLine(
                    "Daedalus::RegisterUdpClient - Error! DaedalusUdpListener doesn't yet support IPv6 addresses! Aborting ...");
                return 0;
            }

            if (_subIdsInuse.Count >= ushort.MaxValue)
            {
                Console.WriteLine(
                    "Daedalus::RegisterUdpClient - Error! DaedalusUdpListener has reached the maximum number of subscribers! Aborting ...");
                return 0;
            }

            ulong epKey = GetEndpointKey(ipEndpoint);
            ushort subId;
            if (_freedSubIds.Count > 0)
            {
                subId = _freedSubIds.First();
                _freedSubIds.Remove(subId);
            }
            else
            {
                subId = (ushort)(_subIdsInuse.Last() + 1);
            }

            ulong subKey = GetSubKey(epKey, subId);

            _udpClients.GetOrAdd(epKey, (_) => { return new UdpClient(ipEndpoint); });
            _subscriberBuffers.GetOrAdd(subKey, new ConcurrentQueue<UdpReceiveResult>());

            _subCallbacks.GetOrAdd(subKey, callback);
            _subIdsInuse.Add(subId);

            Task.Run(() => UpdateUdpListeningLoops(_cancellationTokenSource.Token));

            return subKey;
        }
    }

    /// <summary>
    /// Deregisters a UDP client subscriber and cleans up resources if no other subscribers exist for the endpoint.
    /// </summary>
    /// <param name="subKey">The subscriber key returned from RegisterUdpClient</param>
    public static void DeregisterUdpClient(ulong subKey)
    {
        lock (_registrationLock)
        {
            _subscriberBuffers.TryRemove(subKey, out _);
            _subCallbacks.TryRemove(subKey, out _);

            ushort removedSubId = GetSubIdFromSubKey(subKey);
            _subIdsInuse.Remove(removedSubId);
            _freedSubIds.Add(removedSubId);

            ulong endpointKey = GetEndpointKeyFromSubKey(subKey);

            // Number of subscribers with same endpoint
            int subsLeft = _subscriberBuffers.Keys.Count(_subKey => GetEndpointKeyFromSubKey(_subKey) == endpointKey);
            if (subsLeft == 0)
            {
                _udpClients.TryRemove(endpointKey, out _);
            } 
        }
        
        Task.Run(() => UpdateUdpListeningLoops(_cancellationTokenSource.Token));
    }

    /// <summary>
    /// Retrieves the next available UDP datagram for a subscriber
    /// </summary>
    /// <param name="subKey">The subscriber key</param>
    /// <returns>The next message from the queue, or empty result if none available</returns>
    public static UdpReceiveResult Receive(ulong subKey)
    {
        if (!_subscriberBuffers.TryGetValue(subKey, out ConcurrentQueue<UdpReceiveResult> buff))
        {
            Console.WriteLine($"Unable to find subscriber with ID of {subKey}!");
            return new UdpReceiveResult();
        }

        buff.TryDequeue(out UdpReceiveResult res);
        return res;
    }

    /// <summary>
    /// Encode Address:Port into a 64-bit unsigned number
    /// Starting from most significant bits, each octet will be encoded
    /// The last 16 bits are reserved for a subscriber ID
    ///
    /// E.g., 127.0.0.1:14550 returns:
    ///
    /// (01111111) (00000000) (00000000) (00000001) (00111000 11010110) 00000000 00000000
    ///    (127)  .   (0)    .   (0)    .   (1)    :      (14550)
    /// </summary>
    private static ulong GetEndpointKey(IPEndPoint ipEndpoint)
    {
        byte[] octets = ipEndpoint.Address.GetAddressBytes();
        ulong key = 0;
        for (int i = 0; i < octets.Length; i++)
        {
            key |= (ulong)octets[i] << ((octets.Length - i + 3) * 8);
        }

        key |= (ulong)ipEndpoint.Port << 16;

        return key;
    }

    /// <summary>
    /// See GetEndpointKey above. Simply inserts the subscriber ID in the last 16 bits
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong GetSubKey(ulong endpointKey, ushort subId)
    {
        return endpointKey | (ulong)subId;
    }

    /// <summary>
    /// Extracts the first 48 bits from the subkey, which contains
    /// Address:Port as 4 octets:2 octets
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong GetEndpointKeyFromSubKey(ulong subKey)
    {
        return subKey &= (~(ulong)ushort.MaxValue);
    }

    /// <summary>
    /// Extracts the last 16 bits (subscriber ID) from a subscriber key,
    /// containing Address:Port:SubID
    /// </summary>
    private static ushort GetSubIdFromSubKey(ulong subKey)
    {
        return (ushort)(subKey &= (ulong)ushort.MaxValue);
    }

    private static async Task UpdateUdpListeningLoops(CancellationToken ct)
    {
        KeyValuePair<ulong, UdpClient>[] ipToClientKvps = _udpClients.ToArray();
        foreach (KeyValuePair<ulong, UdpClient> kvp in ipToClientKvps)
        {
            if (!_udpListeningTasks.ContainsKey(kvp.Key))
            {
                _udpListeningTasks[kvp.Key] = Task.Run(() => ListenToClient(kvp.Key, ct));
            }
        }

        IEnumerable<ulong> clientsToRemove = _udpListeningTasks.Keys.ToArray().Except(_udpClients.Keys.ToArray());

        foreach (ulong clientToRemove in clientsToRemove)
        {
            _udpListeningTasks.Remove(clientToRemove, out _);
        }
    }

    private static async Task ListenToClient(ulong udpClientKey, CancellationToken ct)
    {
        _udpClients.TryGetValue(udpClientKey, out UdpClient udpClient);

        if (udpClient == null)
        {
            Console.WriteLine("DaedalusUdpListener::ListenToClient - tried to listen to a client that doesn't exist");
            return;
        }

        while (!ct.IsCancellationRequested && _udpClients.ContainsKey(udpClientKey))
        {
            UdpReceiveResult data;
            try
            {
                data = await udpClient.ReceiveAsync();
            }
            catch (SocketException e)
            {
                Console.WriteLine("DaedalusUdpListener::ListenToClient, ", e);
                return;
            }
            catch (ObjectDisposedException ode)
            {
                Console.WriteLine("DaedalusUdpListener::ListenToClient, ", ode);
                return;
            }

            foreach (KeyValuePair<ulong, ConcurrentQueue<UdpReceiveResult>> subBuffer in _subscriberBuffers)
            {
                ulong ipEndpointKey = GetEndpointKeyFromSubKey(subBuffer.Key);
                if (ipEndpointKey != udpClientKey)
                {
                    continue;
                }

                if (subBuffer.Value.Count > MAX_BUFFER_SIZE)
                {
                    subBuffer.Value.TryDequeue(out _);
                }

                subBuffer.Value.Enqueue(data);

                try
                {
                    _subCallbacks.TryGetValue(subBuffer.Key, out Action<ulong> callback);
                    callback?.Invoke(ipEndpointKey);
                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        "DaedalusUdpListener::ListenToClient, attempt to invoke callback on subscriber which threw an unhandled exception ",
                        e);
                }
            }
        }
    }

    public static void Dispose()
    {
        Console.WriteLine("Destroying DaedalusUdpListener ...");

        _cancellationTokenSource.Cancel();

        foreach (UdpClient client in _udpClients.Values)
        {
            client.Close();
            client.Dispose();
        }

        Console.WriteLine("Finished destroying DaedalusUdpListener!");
    }
}