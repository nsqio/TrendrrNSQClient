package com.trendrr.nsq.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.trendrr.nsq.ConnectionAddress;
import com.trendrr.nsq.model.HostAndPort;
import com.trendrr.nsq.model.LookupResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

//TODO Need refactor AbstractNSQClient...
/**
 * This implementation of {@link NSQLookup} uses Jackson as the underlying Json parser.
 * Moreover, it caches the the connection details of producer of a given topic for a specified time.
 * If no time is supplied, it will cache such details for 5 minutes
 */
public class CachingNSQLookupImpl  implements NSQLookup{

    private static Logger LOGGER = LoggerFactory.getLogger( CachingNSQLookupImpl.class);

    private static final long DEFAULT_LOOKUP_REFRESH_TIME = 5 * 60 * 1000;

    private static final String URL_SUFFIX = "/lookup?topic=";

    private static final String PROTOCOL = "http";

    private final Set<HostAndPort> lookUpAddresses;

    private final ConcurrentMap<String, Set<ConnectionAddress>> topicToConnections;

    private final long refreshInterval;

    public  CachingNSQLookupImpl(){
        this(DEFAULT_LOOKUP_REFRESH_TIME);
    }

    public CachingNSQLookupImpl(long refreshInterval) {
        lookUpAddresses = new CopyOnWriteArraySet<HostAndPort>();
        this.refreshInterval = refreshInterval;
        this.topicToConnections = new ConcurrentHashMap<String, Set<ConnectionAddress>>();
    }

    /**
     * Initialise the lookup refresher thread. For now it is not doing anything as I need to clean up more code in
     * the AbstractNSQClient
     */
    public void init(){
        //TODO Work on AbstractNSQClient so that lookup refresh logic is removed from this class.
    }

    @Override
    public void addAddr(String addr, int port) {
        lookUpAddresses.add(new HostAndPort(addr, port));
    }

    @Override
    public List<ConnectionAddress> lookup(String topic) {
        InputStream is = null;
        Set<ConnectionAddress> connectionAddresses = new HashSet<ConnectionAddress>();
            for(HostAndPort lookupAddress : lookUpAddresses){
                try{
                    URL url = new URL(PROTOCOL, lookupAddress.getHost(), lookupAddress.getPort(), URL_SUFFIX + topic);
                    HttpURLConnection con = (HttpURLConnection) url.openConnection();
                    con.setRequestMethod("GET");
                    int responseCode = con.getResponseCode();

                    if(responseCode == HttpURLConnection.HTTP_OK){
                        ObjectMapper mapper = new ObjectMapper();
                        is = con.getInputStream();
                        LookupResponse response = mapper.readValue(is, LookupResponse.class);
                        populateConnections(connectionAddresses, response.getLookupData());
                        is.close();
                    }else{
                        LOGGER.warn("Received a response code that is not 200 [response-code=" + responseCode + ", lookup-address=" +
                                lookupAddress + ", topic=" + topic + "]");
                    }
                }catch(Exception e){
                    LOGGER.error("An error occurred when trying to get producers for topic [lookup-addresses=" + lookupAddress +
                        ", topic=" + topic + "]", e);
                }finally {
                    try{
                        if(is != null){
                            is.close();
                        }
                    }catch(Exception e){
                        LOGGER.error("An error occurred while trying to close input stream [lookup-address=" + lookupAddress +
                                ", topic=" + topic + "]", e);
                    }
                }
            }

         topicToConnections.put(topic, connectionAddresses);
        return Lists.newArrayList(connectionAddresses);
    }

    private void populateConnections(Set<ConnectionAddress> connections,
                                                       LookupResponse.LookupData lookupData){
        LookupResponse.ProducerDetails[] producers = lookupData.getProducers();
        if(producers == null){
            return;
        }
        for(LookupResponse.ProducerDetails producerDetails : producers){
            connections.add(new ConnectionAddress(producerDetails.getBroadcastAddress(), producerDetails.getTcpPort()));
        }
    }
}
