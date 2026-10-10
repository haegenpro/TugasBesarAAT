package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"time"

	"aggregator/internal/avrowire"
	"github.com/gin-gonic/gin"
)

// RegisterSchemaAdmin exposes an operator control on the internal Aggregator,
// separate from downstream client credentials and the PVMBG writer toggle.
func RegisterSchemaAdmin(r *gin.Engine, readers *avrowire.ReaderSwitch, token string) {
	r.POST("/admin/schema-version", func(c *gin.Context) {
		if token == "" || subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+token)) != 1 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid admin credentials"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()
		version, err := readers.Toggle(ctx)
		if err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, avrowire.ErrReaderCandidateUnavailable) {
				status = http.StatusConflict
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"reader_version": version})
	})
}
