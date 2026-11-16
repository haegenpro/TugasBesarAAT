package coastal

import (
	"aggregator/internal/rawinput"
	"net/http"

	"github.com/gin-gonic/gin"
)

func RegisterStatus(r gin.IRoutes, status *Status, token string) {
	r.GET("/internal/coastal/status", func(c *gin.Context) {
		if !rawinput.Authorize(c, token) {
			return
		}
		c.JSON(http.StatusOK, status.Snapshot())
	})
}
