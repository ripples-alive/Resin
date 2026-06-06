package api

import (
	"net/http"

	"github.com/Resinat/Resin/internal/service"
)

func subscriptionListPageResponse(result service.SubscriptionListResult) PageResponse[service.SubscriptionResponse] {
	return PageResponse[service.SubscriptionResponse]{
		Items:  result.Items,
		Total:  result.Total,
		Limit:  result.Limit,
		Offset: result.Offset,
	}
}

// HandleListSubscriptions returns a handler for GET /api/v1/subscriptions.
func HandleListSubscriptions(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		active, ok := parseBoolQueryOrWriteInvalid(w, r, "active")
		if !ok {
			return
		}
		enabled, ok := parseBoolQueryOrWriteInvalid(w, r, "enabled")
		if !ok {
			return
		}
		sorting, ok := parseSortingOrWriteInvalid(
			w,
			r,
			[]string{"name", "created_at", "last_checked", "last_updated"},
			"created_at",
			"asc",
		)
		if !ok {
			return
		}
		pg, ok := parsePaginationOrWriteInvalid(w, r)
		if !ok {
			return
		}

		result, err := cp.ListSubscriptions(
			service.SubscriptionListFilters{
				Active:  active,
				Enabled: enabled,
				Keyword: r.URL.Query().Get("keyword"),
			},
			service.SubscriptionListOptions{
				SortBy:    sorting.SortBy,
				SortOrder: sorting.SortOrder,
				Limit:     pg.Limit,
				Offset:    pg.Offset,
			},
		)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, subscriptionListPageResponse(result))
	}
}

// HandleGetSubscription returns a handler for GET /api/v1/subscriptions/{id}.
func HandleGetSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		s, err := cp.GetSubscription(id)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, s)
	}
}

// HandleCreateSubscription returns a handler for POST /api/v1/subscriptions.
func HandleCreateSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req service.CreateSubscriptionRequest
		if err := DecodeBody(r, &req); err != nil {
			writeDecodeBodyError(w, err)
			return
		}
		s, err := cp.CreateSubscription(req)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusCreated, s)
	}
}

// HandleUpdateSubscription returns a handler for PATCH /api/v1/subscriptions/{id}.
func HandleUpdateSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		body, ok := readRawBodyOrWriteInvalid(w, r)
		if !ok {
			return
		}
		s, err := cp.UpdateSubscription(id, body)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, s)
	}
}

// HandleDeleteSubscription returns a handler for DELETE /api/v1/subscriptions/{id}.
func HandleDeleteSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		if err := cp.DeleteSubscription(id); err != nil {
			writeServiceError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// HandleRefreshSubscription returns a handler for POST /api/v1/subscriptions/{id}/actions/refresh.
func HandleRefreshSubscription(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		if err := cp.RefreshSubscription(id); err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// HandleCleanupSubscriptionCircuitOpenNodes returns a handler for
// POST /api/v1/subscriptions/{id}/actions/cleanup-circuit-open-nodes.
func HandleCleanupSubscriptionCircuitOpenNodes(cp *service.ControlPlaneService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := requireUUIDPathParam(w, r, "id", "subscription_id")
		if !ok {
			return
		}
		cleanedCount, err := cp.CleanupSubscriptionCircuitOpenNodes(id)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]int{"cleaned_count": cleanedCount})
	}
}
