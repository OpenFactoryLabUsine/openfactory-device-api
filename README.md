# openfactory-device-api
API interne à OpenFactory qui expose les données des capteurs connectés

# Comment ça fonctionne
L'API expose un serveur WebSocket qui permet de suivre en temps réel l'état des appareils enregistrés dans OpenFactory.

## Points de connexion

### `GET /ws/devices`

Retourne la liste de tous les appareils disponibles et leur état courant. La connexion est maintenue active par des pings périodiques.

### `GET /ws/devices/{device_uuid}`

Ouvre une connexion persistante vers un appareil spécifique. Le serveur diffuse les mises à jour en temps réel au fur et à mesure que les données de l'appareil changent.

---

## Messages du serveur

Tous les messages sont des objets JSON avec un champ `event` qui identifie le type de message.

---

### `device_update`

Envoyé immédiatement lors de la connexion avec l'état complet et courant de l'appareil, puis renvoyé à chaque changement de données.

```json
{
  "event": "device_update",
  "device_uuid": "DEVICE-1",
  "timestamp": 1748000000.0,
  "items": [
    {
      "id": "DEVICE-1-temperature",
      "value": "23.4",
      "kind": "sample",
      "timestamp": "2024-01-01T12:00:00.0000000",
      "meta": {}
    },
    {
      "id": "stat:on",
      "value": 3600,
      "kind": "stat",
      "timestamp": null,
      "meta": {}
    },
    {
      "id": "avg:DEVICE-1-temperature",
      "value": "22.1",
      "kind": "avg",
      "timestamp": "2024-01-01T12:00:00.0000000",
      "meta": {}
    }
  ]
}
```

**Types d'items**

| Type       | Description                                                                                   |
|------------|-----------------------------------------------------------------------------------------------|
| `sample`   | Valeur d'un dataitem(variable) de l'appareil                                                  |
| tout autre | Donnée propre à une variable, issue d'un traitement venant de OpenFactory                     |

---

### `devices_list`

Envoyé une seule fois lors de la connexion a `/ws/devices`, avec l'ensemble des appareils connus et leur état courant.

```json
{
  "event": "devices_list",
  "timestamp": 1748000000.0,
  "devices": [
    {
      "device_uuid": "DEVICE-1",
      "dataitems": {
        "DEVICE-1-temperature": "23.4"
      },
      "durations": {}
    }
  ]
}
```

---

### `ping`

Envoyé périodiquement pour maintenir la connexion active. Sur `/ws/devices`, inclut le nombre d'appareils actifs.

```json
{
  "event": "ping",
  "active_devices": 3,
  "timestamp": 1748000000.0
}
```

---

### `simulation_mode_updated`

Envoyé en réponse à un message client `simulation_mode`.

```json
{
  "event": "simulation_mode_updated",
  "value": true,
  "success": true
}
```

En cas d'échec, `success` est `false` et un champ `error` est inclus :

```json
{
  "event": "simulation_mode_updated",
  "value": true,
  "success": false,
  "error": "Failed to send method"
}
```

---

### `stream_dropped`

Envoyé en réponse à un message client `drop_stream`.

```json
{
  "event": "stream_dropped",
  "device_uuid": "DEVICE-1",
  "success": true,
  "timestamp": 1748000000.0
}
```

---

### `error`

Envoyé lorsque le serveur rencontre une erreur en traitant une requête.

```json
{
  "event": "error",
  "message": "Unknown method: foo",
  "timestamp": 1748000000.0
}
```

---

## Messages du client

Envoyés par le client sur une connexion active vers `/ws/devices/{device_uuid}`.

Tous les messages client suivent cette structure :

```json
{
  "method": "<nom_de_la_methode>",
  "params": {}
}
```

---

### `simulation_mode`

Active ou desactive le mode simulation des commandes envoyées à OpenFactory.

```json
{
  "method": "simulation_mode",
  "params": {
    "name": "simulationMode",
    "args": true
  }
}
```

| Champ          | Type      | Description                              |
|----------------|-----------|------------------------------------------|
| `params.name`  | `string`  | Nom de la méthode a invoquer             |
| `params.args`  | `boolean` | `true` pour activer, `false` pour désactiver |

---

### `drop_stream`

Arrête le flux Kafka dédié de l'appareil et ferme la surveillance.

```json
{
  "method": "drop_stream",
  "params": {}
}
```

Aucun paramêtre requis.
