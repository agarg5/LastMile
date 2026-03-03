import type { Order, Driver, Merchant, Shift } from "../types";

const API_BASE =
  import.meta.env.VITE_API_URL || "http://localhost:8000";

// Simple auth storage in localStorage
const AUTH_STORAGE_KEY = "tms_auth";

export interface AuthInfo {
  accessToken: string;
  merchant: Merchant;
}

export function getAuth(): AuthInfo | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(AUTH_STORAGE_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as AuthInfo;
  } catch {
    return null;
  }
}

export function setAuth(auth: AuthInfo) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(auth));
}

export function clearAuth() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(AUTH_STORAGE_KEY);
}

async function request<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const auth = getAuth();
  const authValue = auth?.accessToken ? `Bearer ${auth.accessToken}` : null;
  const authHeaders: Record<string, string> | undefined = authValue
    ? { Authorization: authValue, Autorization: authValue }
    : undefined;

  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(authHeaders ?? {}),
      ...options?.headers,
    },
  });

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ error: "Unknown error" }));
    throw new Error(error.error || `HTTP ${response.status}`);
  }

  return response.json();
}

export const api = {
  // Auth
  login: (email: string, password: string) =>
    request<{
      access_token: string;
      token_type: string;
      expires_in: number;
      merchant: Merchant;
    }>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    }).then((data) => {
      setAuth({ accessToken: data.access_token, merchant: data.merchant });
      return data;
    }),

  // Orders
  getOrders: (merchantId: number, page = 1, perPage = 50, search?: string) => {
    const searchParam = search ? `&search=${encodeURIComponent(search)}` : "";
    return request<{
      orders: Order[];
      total: number;
      page: number;
      per_page: number;
      total_pages: number;
    }>(
      `/orders?merchant_id=${merchantId}&page=${page}&per_page=${perPage}${searchParam}`
    );
  },

  createOrder: (data: {
    merchant_id: number;
    description?: string;
    pickup_time: string;
    dropoff_time: string;
    weight: number;
  }) =>
    request<Order>("/orders", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  updateOrder: (
    orderId: number,
    data: {
      merchant_id: number;
      description?: string;
      pickup_time?: string;
      dropoff_time?: string;
      weight?: number;
    }
  ) =>
    request<Order>(`/orders/${orderId}`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  cancelOrder: (orderId: number) =>
    request<{ message: string }>(`/orders/${orderId}`, {
      method: "DELETE",
    }),

  // Drivers
  getDrivers: () => request<Driver[]>("/drivers"),

  // Merchants
  getMerchants: () => request<Merchant[]>("/merchants"),

  // Shifts
  getShifts: () => request<Shift[]>("/shifts"),

  // CSV Upload
  uploadCSV: (
    file: File,
    type: "merchants" | "drivers" | "vehicles" | "orders"
  ) => {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("type", type);

    return fetch(`${API_BASE}/upload`, {
      method: "POST",
      body: formData,
    }).then((res) => {
      if (!res.ok) {
        return res.json().then((err) => {
          throw new Error(err.error || "Upload failed");
        });
      }
      return res.json();
    });
  },
};
