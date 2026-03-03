import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useState } from "react";
import { MapContainer, Marker, Popup, TileLayer } from "react-leaflet";
import { io } from "socket.io-client";
import type { LocationUpdate } from "../types";

// Fix for default marker icon
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl:
    "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png",
  iconUrl:
    "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png",
  shadowUrl:
    "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
});

export default function TrackingPage() {
  const [locations, setLocations] = useState<Map<number, LocationUpdate>>(
    new Map()
  );
  const [connected, setConnected] = useState(false);

  const socketUrl =
    import.meta.env.VITE_API_URL || "http://localhost:8000";

  useEffect(() => {
    // Connect to Socket.IO server
    const socket = io(socketUrl, {
      transports: ["websocket", "polling"],
    });

    socket.on("connect", () => {
      console.log("WebSocket connected");
      setConnected(true);
    });

    socket.on("location_update", (update: LocationUpdate) => {
      setLocations((prev) => {
        const newMap = new Map(prev);
        newMap.set(update.driver_id, update);
        return newMap;
      });
    });

    socket.on("disconnect", () => {
      console.log("WebSocket disconnected");
      setConnected(false);
    });

    socket.on("connect_error", (error) => {
      console.error("WebSocket connection error:", error);
      setConnected(false);
    });

    return () => {
      socket.disconnect();
    };
  }, [socketUrl]);

  const locationArray = Array.from(locations.values());
  const center: [number, number] =
    locationArray.length > 0
      ? [locationArray[0].latitude, locationArray[0].longitude]
      : [40.7128, -74.006]; // Default to NYC

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-3xl font-bold">Order Tracking</h1>
        <div className="flex items-center gap-2">
          <div
            className={`h-3 w-3 rounded-full ${
              connected ? "bg-green-500" : "bg-red-500"
            }`}
          />
          <span className="text-sm text-muted-foreground">
            {connected ? "Connected" : "Disconnected"}
          </span>
        </div>
      </div>

      <div className="rounded-md border">
        <div className="h-[600px] w-full">
          <MapContainer
            center={center}
            zoom={13}
            style={{ height: "100%", width: "100%" }}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {locationArray.map((location) => (
              <Marker
                key={location.driver_id}
                position={[location.latitude, location.longitude]}
              >
                <Popup>
                  <div>
                    <div className="font-semibold">{location.driver_name}</div>
                    <div className="text-sm text-muted-foreground">
                      Driver ID: {location.driver_id}
                    </div>
                    <div className="text-sm text-muted-foreground">
                      Updated:{" "}
                      {new Date(location.timestamp).toLocaleTimeString()}
                    </div>
                  </div>
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        </div>
      </div>

      <div className="rounded-md border p-4">
        <h2 className="mb-4 text-lg font-semibold">Active Drivers</h2>
        {locationArray.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No active drivers at the moment
          </p>
        ) : (
          <div className="space-y-2">
            {locationArray.map((location) => (
              <div
                key={location.driver_id}
                className="flex items-center justify-between rounded border p-2"
              >
                <div>
                  <div className="font-medium">{location.driver_name}</div>
                  <div className="text-sm text-muted-foreground">
                    {location.latitude.toFixed(4)},{" "}
                    {location.longitude.toFixed(4)}
                  </div>
                </div>
                <div className="text-sm text-muted-foreground">
                  {new Date(location.timestamp).toLocaleTimeString()}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
