package coastal

import (
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func RegisterStatus(r gin.IRoutes, status *Status, token string, repositories ...*Repository) {
	r.GET("/internal/coastal/status", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		if len(repositories) > 0 && repositories[0] != nil {
			ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
			defer cancel()
			v, err := repositories[0].Status(ctx)
			if err != nil {
				rawinput.Error(c, canonical.Unavailable())
				return
			}
			c.JSON(http.StatusOK, v)
			return
		}
		c.JSON(http.StatusOK, status.Snapshot())
	})
}

func RegisterExpectations(r gin.IRoutes, repo *Repository, token string) {
	r.GET("/internal/coastal/warnings/expected", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		since, err := time.Parse(time.RFC3339Nano, c.Query("since"))
		if err != nil {
			rawinput.Error(c, canonical.Fail(400, "invalid_request", "since required"))
			return
		}
		var after, through int64
		var asOf time.Time
		if c.Query("after") != "" {
			after, err = strconv.ParseInt(c.Query("after"), 10, 64)
			if err != nil || after < 0 {
				rawinput.Error(c, canonical.Fail(400, "invalid_request", "invalid after"))
				return
			}
			through, err = strconv.ParseInt(c.Query("through"), 10, 64)
			if err != nil || through < after {
				rawinput.Error(c, canonical.Fail(400, "invalid_request", "invalid through"))
				return
			}
			asOf, err = time.Parse(time.RFC3339Nano, c.Query("as_of"))
			if err != nil {
				rawinput.Error(c, canonical.Fail(400, "invalid_request", "invalid as_of"))
				return
			}
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
		defer cancel()
		page, err := repo.Expectations(ctx, since, after, through, asOf)
		if err != nil {
			rawinput.Error(c, canonical.Unavailable())
			return
		}
		c.JSON(200, page)
	})
}
