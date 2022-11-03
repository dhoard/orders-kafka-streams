# orders-kafka-streams

EXAMPLE CODE ONLY

NO SUPPORT PROVIDED

USE AT YOUR OWN RISK

---

Topics required :
 - order
 - order-keyed
 - order-info
 - facility-info
 - facility-info-by-minute

---

Source topic :

`order`

Source events :

`order.placed` event

```
{
	"event.type": "order.placed",
	"event.timestamp": 1667503309938,
	"facility.id": "facility-1",
	"order.id": "dc16fa3c-1beb-4992-8c27-173856fb0cdf"
}
```

`order.fulfilled` event

```
{
	"event.type": "order.fulfilled",
	"event.timestamp": 1667503309423,
	"facility.id": "facility-1",
	"order.id": "dc16fa3c-1beb-4992-8c27-173856fb0cdf"
}
```

Output topic :

`facility-info-by-minute`

Output event :

```
{
  "event.type": "facility.info",
  "facility.id": "facility-1",
  "processing.count": 13,
  "processing.ms": 613
}
```

`processing.count` - count of orders processed within a facility within the last minute

`processing.ms` - sum of processing time for orders within a facility within the last minute


