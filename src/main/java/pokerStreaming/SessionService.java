package pokerStreaming;

import org.apache.spark.streaming.State;
import toBeDeleted.Widget;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;


public class SessionService {

    public static void openLogin (Event event, State<Session> sessionState) {

        Session session = new Session();

        session.loginEvent = event;
        session.lastServerDatetime = event.serverDateTime;

        // write to big query
        EventService.writeToDestination(session.loginEvent);

        sessionState.update(session);
    }

    public static void closeWidget(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (/*!session.windowEvents.isEmpty()
                && */!Objects.isNull(session.windowEvents.get(event.windowId))) {

            Event widgetInSession = session.windowEvents.get(event.windowId).widgetEvents.get(event.object);

            if (Objects.isNull(widgetInSession)) {

                // widget not exists for window in state
                event.serverToDateTime = event.serverDateTime;
                event.serverDateTime = null;
                event.clientToDateTime = event.clientDateTime;
                event.clientDateTime = null;

                // write to big query widget data
                EventService.writeToDestination(event);

                session.windowEvents.get(event.windowId).widgetEvents.put(event.object, event);
                sessionState.update(session);
            }
            // state: widget open, event: widget close
            else if (widgetInSession.action.equals("open")) {

                widgetInSession.serverToDateTime = event.serverDateTime;
                widgetInSession.clientToDateTime = event.clientDateTime;
                widgetInSession.closeReason = event.closeReason;

                widgetInSession.screen = session.loginEvent.screen;
                widgetInSession.screen1Width = session.loginEvent.screen1Width;
                widgetInSession.screen1Height = session.loginEvent.screen1Height;
                widgetInSession.cid = session.loginEvent.cid;
                widgetInSession.loginId = session.loginEvent.loginId;

                // write to big query
                EventService.writeToDestination(widgetInSession);

                // update state - remove widget
                session.windowEvents.get(event.windowId).widgetEvents.remove(event.object);
                sessionState.update(session);
            }
        }
    }

    public static void openWidget(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (!session.windowEvents.isEmpty()
                && !Objects.isNull(session.windowEvents.get(event.windowId))) {

            // New Widget in Window
            Event widgetInSession = session.windowEvents.get(event.windowId).widgetEvents.get(event.object);

            if (Objects.isNull(widgetInSession)) {
                session.windowEvents.get(event.windowId).widgetEvents.put(event.object, event);

                // write to big query widget data
                EventService.writeToDestination(event);

                sessionState.update(session);

            }
            // State: close widget, Event: widget open
            else if (widgetInSession.action.equals("close")
                    && event.serverDateTime.isBefore(widgetInSession.serverToDateTime)) {

                widgetInSession.serverDateTime = event.serverDateTime;
                widgetInSession.clientDateTime = event.clientDateTime;

                widgetInSession.screen = session.loginEvent.screen;
                widgetInSession.screen1Width = session.loginEvent.screen1Width;
                widgetInSession.screen1Height = session.loginEvent.screen1Height;
                widgetInSession.cid = session.loginEvent.cid;
                widgetInSession.loginId = session.loginEvent.loginId;

                // write to big query widget data
                EventService.writeToDestination(widgetInSession);

                // update state - remove window
                session.windowEvents.get(event.windowId).widgetEvents.remove(event.object);

                sessionState.update(session);
            }
        }
    }

    public static void closeWindow(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (/*!session.windowEvents.isEmpty()
                && */!Objects.isNull(session.windowEvents.get(event.windowId))) {

            Event windowInSession = session.windowEvents.get(event.windowId).windowEvent;

            if (windowInSession.action.equals("open")) {
                // add close data & login data
                System.out.println("********** state: window open; event: close **********");

                windowInSession.serverToDateTime = event.serverDateTime;
                windowInSession.clientToDateTime = event.clientDateTime;
                windowInSession.closeReason = event.closeReason;

                windowInSession.screen = session.loginEvent.screen;
                windowInSession.screen1Width = session.loginEvent.screen1Width;
                windowInSession.screen1Height = session.loginEvent.screen1Height;
                windowInSession.cid = session.loginEvent.cid;
                windowInSession.loginId = session.loginEvent.loginId;

                // close widgets if exists (and write to big query)
                HashMap<String, Event> widgetEvents = session.windowEvents.get(event.windowId).widgetEvents;
                if (!Objects.isNull(widgetEvents)
                        && !widgetEvents.isEmpty()) {

                    event.objectType = "widget";
                    for (Map.Entry<String, Event> pair : widgetEvents.entrySet()) {

                        event.object = pair.getKey();
                        closeWidget(event, sessionState);
                    }
                }

                // write to big query window data
                EventService.writeToDestination(windowInSession);

                // update state - remove window
                session.windowEvents.remove(event.windowId);

                sessionState.update(session);
            }
        }
        else {
            System.out.println("**********\n state: no window; event: close window\n**********");

            event.serverToDateTime = event.serverDateTime;
            event.serverDateTime = null;
            event.clientToDateTime = event.clientDateTime;
            event.clientDateTime = null;

            EventService.writeToDestination(event);

            Window window = new Window();
            window.windowEvent = event;
            session.windowEvents.put(event.windowId, window);

            sessionState.update(session);

        }
    }

    public static void openWindow(Event event, State<Session> sessionState) {

        Session session = sessionState.get();

        if (!session.windowEvents.isEmpty()
                && !Objects.isNull(session.windowEvents.get(event.windowId))) {

            Event windowInSession = session.windowEvents.get(event.windowId).windowEvent;

            if (!Objects.isNull(windowInSession)) {
                if (windowInSession.action.equals("close")
                        && event.serverDateTime.isBefore(windowInSession.serverToDateTime)) {

                    System.out.println("**********\n state: window close; event: open; open came before close\n**********");

                    windowInSession.serverDateTime = event.serverDateTime;
                    windowInSession.clientDateTime = event.clientDateTime;

                    windowInSession.screen = session.loginEvent.screen;
                    windowInSession.screen1Width = session.loginEvent.screen1Width;
                    windowInSession.screen1Height = session.loginEvent.screen1Height;
                    windowInSession.cid = session.loginEvent.cid;
                    windowInSession.loginId = session.loginEvent.loginId;

                    // close widgets if exists (and write to big query)
                    HashMap<String, Event> widgetEvents = session.windowEvents.get(event.windowId).widgetEvents;
                    if (!Objects.isNull(widgetEvents)
                            && !widgetEvents.isEmpty()) {

                        for (Map.Entry<String, Event> pair : widgetEvents.entrySet()) {
                            closeWidget((Event) pair.getValue(), sessionState);
                        }
                    }

                    // write to big query window data
                    EventService.writeToDestination(windowInSession);

                    // update state - remove window
                    session.windowEvents.remove(event.windowId);

                    sessionState.update(session);
                }
            }
        }
        else {
            System.out.println("**********\n state: no window; event: open\n**********");
            EventService.writeToDestination(event);

            Window window = new Window();
            window.windowEvent = event;
            session.windowEvents.put(event.windowId, window);

            sessionState.update(session);
        }
    }

    public static void preferredSeat (Event event, State<Session> sessionState) {

        System.out.println("got here");
        Session session = sessionState.get();
        session.lastServerDatetime = event.serverDateTime;

        event.object += "_" + event.preferredSeatTableNum;

        // write to big query
        EventService.writeToDestination(event);

        sessionState.update(session);
    }

    public static void eventWithoutDuration (Event event, State<Session> sessionState) {

        Session session = sessionState.get();
        session.lastServerDatetime = event.serverDateTime;

        // write to big query
        EventService.writeToDestination(event);

        sessionState.update(session);
    }
}

