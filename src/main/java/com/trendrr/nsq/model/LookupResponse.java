package com.trendrr.nsq.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

/**
 * Models the response sent my NSQ upon lookup on a given topic.
 * The class is annotated so that Jackson can easily populate it with
 * json obtained from nsqlookupd
 */
public class LookupResponse {

    private int statusCode;

    private String statusText;

    private LookupData lookupData;

    @JsonProperty("status_code")
    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    @JsonProperty("status_txt")
    public String getStatusText() {
        return statusText;
    }

    public void setStatusText(String statusText) {
        this.statusText = statusText;
    }

    @JsonProperty("data")
    public LookupData getLookupData() {
        return lookupData;
    }

    public void setLookupData(LookupData lookupData) {
        this.lookupData = lookupData;
    }

    public static class LookupData{

        private String[] channels;

        private ProducerDetails[] producers;

        @JsonProperty("channels")
        private String[] getChannels() {
            return channels;
        }

        private void setChannels(String[] channels) {
            this.channels = channels;
        }

        @JsonProperty("producers")
        public ProducerDetails[] getProducers() {
            return producers;
        }

        public void setProducers(ProducerDetails[] producers) {
            this.producers = producers;
        }

        @Override
        public String toString() {
            return "LookupData{" +
                    "channels=" + Arrays.toString(channels) +
                    ", producers=" + Arrays.toString(producers) +
                    '}';
        }
    }

    public static class ProducerDetails{
        private String remoteAddress;

        private String address;

        private String hostName;

        private String broadcastAddress;

        private int tcpPort;

        private int httpPort;

        private String version;

        @JsonProperty("remote_address")
        public String getRemoteAddress() {
            return remoteAddress;
        }

        public void setRemoteAddress(String remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        @JsonProperty("address")
        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        @JsonProperty("hostname")
        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        @JsonProperty("broadcast_address")
        public String getBroadcastAddress() {
            return broadcastAddress;
        }

        public void setBroadcastAddress(String broadcastAddress) {
            this.broadcastAddress = broadcastAddress;
        }


        @JsonProperty("tcp_port")
        public int getTcpPort() {
            return tcpPort;
        }

        public void setTcpPort(int tcpPort) {
            this.tcpPort = tcpPort;
        }

        @JsonProperty("http_port")
        public int getHttpPort() {
            return httpPort;
        }

        public void setHttpPort(int httpPort) {
            this.httpPort = httpPort;
        }

        @JsonProperty("version")
        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return "ProducerDetails{" +
                    "remoteAddress='" + remoteAddress + '\'' +
                    ", address='" + address + '\'' +
                    ", hostName='" + hostName + '\'' +
                    ", broadcastAddress='" + broadcastAddress + '\'' +
                    ", tcpPort=" + tcpPort +
                    ", httpPort=" + httpPort +
                    ", version='" + version + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "LookupResponse{" +
                "statusCode=" + statusCode +
                ", statusText='" + statusText + '\'' +
                ", lookupData=" + lookupData +
                '}';
    }
}
