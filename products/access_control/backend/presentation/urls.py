"""URL routes for field access controls on property definitions."""

from rest_framework.routers import DefaultRouter

from .views import PropertyAccessControlViewSet

router = DefaultRouter()
router.register(r"", PropertyAccessControlViewSet, basename="property-access-controls")
urlpatterns = router.urls
