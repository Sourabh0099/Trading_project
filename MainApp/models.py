from django.db import models

# Create your models here.

class Candle(models.Model):
    instrument = models.CharField(max_length=100)
    date = models.DateField()
    time = models.TimeField()
    open = models.DecimalField(max_digits=10, decimal_places=2)
    high = models.DecimalField(max_digits=10, decimal_places=2)
    low = models.DecimalField(max_digits=10, decimal_places=2)
    close = models.DecimalField(max_digits=10, decimal_places=2)
    volume = models.IntegerField()

    def __str__(self):
        return f"{self.instrument} - {self.date} {self.time}"
