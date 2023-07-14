from django.db import models

# Create your models here.
from django.db import models


# Create your models here.
class Quote(models.Model):
    title = models.CharField(max_length=50)

    likes = models.PositiveIntegerField(default=0)

    def __dir__(self):
        return self.title


class Message(models.Model):
    content = models.TextField()
    queue = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['created_at']
