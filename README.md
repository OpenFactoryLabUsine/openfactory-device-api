# openfactory-equipment-api
API interne à OpenFactory qui expose les données des capteurs connectés

# Comment ça fonctionne
L'API expose un serveur WebSocket qui permet de suivre en temps réel l'état des appareils enregistrés dans OpenFactory.

## Points de connexion

### `GET /ws/equipments`

Retourne la liste de tous les appareils disponibles et leur état courant. La connexion est maintenue active par des pings périodiques.

### `GET /ws/equipments/{asset_uuid}`

Ouvre une connexion persistante vers un appareil spécifique. Le serveur diffuse les mises à jour en temps réel au fur et à mesure que les données de l'appareil changent.

---

## Messages du serveur

Tous les messages sont des objets JSON avec un champ `event` qui identifie le type de message.

---

### `equipment_update`

Envoyé immédiatement lors de la connexion avec l'état complet et courant de l'appareil, puis renvoyé à chaque changement de données.

```json
{
  "event": "equipment_update",
  "asset_uuid": "EQUIPMENT-1",
  "timestamp": 1748000000.0,
  "variables": [
    {
      "id": "EQUIPMENT-1-temperature",
      "value": "23.4",
      "kind": "sample",
      "timestamp": "2026-01-01T12:00:00.0000000"
    },
    {
      "id": "stat:on",
      "value": 3600,
      "kind": "stat",
      "timestamp": null
    },
    {
      "id": "avg:EQUIPMENT-1-temperature",
      "value": "22.1",
      "kind": "avg",
      "timestamp": "2026-01-01T12:00:00.0000000"
    }
  ]
}
```

**Types de variables**

| Type       | Description                                                                                   |
|------------|-----------------------------------------------------------------------------------------------|
| `sample`   | Valeur d'une variable de l'équipement                                                         |
| tout autre | Donnée propre à une variable, issue d'un traitement venant de OpenFactory                     |

---

### `equipments_list`

Envoyé une seule fois lors de la connexion a `/ws/equipments`, avec l'ensemble des appareils connus et leur état courant.

```json
{
  "event": "equipments_list",
  "timestamp": 1748000000.0,
  "equipments": [
    {
      "asset_uuid": "EQUIPMENT-1",
      "variables": {
        "EQUIPMENT-1-temperature": "23.4"
      }
    }
  ]
}
```

---

### `ping`

Envoyé périodiquement pour maintenir la connexion active. Sur `/ws/equipments`, inclut le nombre d'appareils actifs.

```json
{
  "event": "ping",
  "active_equipments": 3,
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

### `connection_dropped`

Envoyé en réponse à un message client `drop_connection`.

```json
{
  "event": "connection_dropped",
  "asset_uuid": "EQUIPMENT-1",
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

Envoyés par le client sur une connexion active vers `/ws/equipments/{asset_uuid}`.

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

### `drop_connection`

Arrête le flux Kafka dédié de l'appareil et ferme la surveillance.

```json
{
  "method": "drop_connection",
  "params": {}
}
```

Aucun paramêtre requis.
