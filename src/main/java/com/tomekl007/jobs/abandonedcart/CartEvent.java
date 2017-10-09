package com.tomekl007.jobs.abandonedcart;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.ZonedDateTime;

public class CartEvent {
    private ZonedDateTime eventTime;
    private String userId;
    private String typeOfEvent;

    public CartEvent() {
    }

    public CartEvent(ZonedDateTime eventTime, String userId, String typeOfEvent) {
        this.userId = userId;
        this.typeOfEvent = typeOfEvent;
        this.eventTime = eventTime;
    }

    public String getUserId() {
        return userId;
    }

    public String getTypeOfEvent() {
        return typeOfEvent;
    }

    public ZonedDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(ZonedDateTime eventTime) {
        this.eventTime = eventTime;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setTypeOfEvent(String typeOfEvent) {
        this.typeOfEvent = typeOfEvent;
    }

    @Override
    public String toString() {
        return "CartEvent{" +
                "eventTime=" + eventTime +
                ", userId='" + userId + '\'' +
                ", typeOfEvent='" + typeOfEvent + '\'' +
                '}';
    }
}
