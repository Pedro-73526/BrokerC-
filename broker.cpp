/***************************************************************
 * main.cpp
 * Exemplo de "lógica de broker" em C++,
 * usando Paho MQTT C++ e nlohmann/json.
 *
 * Para compilar (assumindo que Paho C++ e nlohmann/json
 * estão instalados em /usr/local):
 *
 *   g++ -std=c++17 main.cpp -o mqtt_logic \
 *       -I/usr/local/include -L/usr/local/lib \
 *       -lpaho-mqttpp3 -lpaho-mqtt3as
 *
 * Execute primeiro o broker Mosquitto:
 *   sudo systemctl start mosquitto
 *
 * Depois execute:
 *   ./mqtt_logic
 *
 ***************************************************************/

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <thread>

// Bibliotecas MQTT
#include "mqtt/async_client.h"

// Biblioteca JSON (header-only). Certifique-se de que está instalada,
// ou coloque o single-header nlohmann/json.hpp no seu projeto.
#include <nlohmann/json.hpp>

// Para simplificar o uso:
using json = nlohmann::json;

/* -----------------------------------------------------------------------
   Estruturas equivalentes ao código .NET:
   - CanData
   - CanMessage (normal)
   - CanMessageSimulator (sim)
   -----------------------------------------------------------------------*/
struct CanData {
    int ArbitrationId;
    std::vector<int> Data;
};

// Representa "can/messages"
struct CanMessage {
    std::string AlgorithmID;
    CanData CAN_Message;
};

// Representa "sim/canmessages"
struct CanMessageSimulator {
    std::string AlgorithmID;
    CanData CAN_Message;
};

/* -----------------------------------------------------------------------
   jsonMessage final, equivalente a:
     public class JsonMessage
   -----------------------------------------------------------------------*/
struct JsonMessage {
    std::string AlgorithmID;
    std::string Timestamp;
    bool Status;
    json Data; // "Data" pode variar (objeto anônimo)
};

/* -----------------------------------------------------------------------
   Funções para converter CAN -> JSON, como no código .NET
   -----------------------------------------------------------------------*/
JsonMessage canToJson(const CanMessage& msg) {
    // Extraindo dados
    int arbitrationId = msg.CAN_Message.ArbitrationId;
    const auto &data  = msg.CAN_Message.Data;

    std::string algorithmId = msg.AlgorithmID.empty()
                              ? "Unknown"
                              : msg.AlgorithmID;

    bool status = (data.size() > 0 && data[0] == 1);
    int distance = 0;
    if (data.size() > 2) {
        distance = (data[2] << 8) | data[1];
    }

    // Caso especial: BlindSpotDetection => "Side"
    std::string side = "Esquerda";
    if (algorithmId == "BlindSpotDetection" && data.size() > 3 && data[3] == 1) {
        side = "Direita";
    }

    // Monta o JsonMessage
    JsonMessage result;
    result.AlgorithmID = algorithmId;
    // Timestamp ISO 8601 (simplificado)
    // Você poderia usar <chrono> e formatar adequadamente.
    result.Timestamp   = "2025-01-31T12:00:00Z";
    result.Status      = status;

    if (algorithmId == "BlindSpotDetection") {
        result.Data = {
            {"Side", side},
            {"DistanceToVehicle", distance / 100.0}
        };
    } else {
        result.Data = {
            {"DistanceToVehicle", distance / 100.0}
        };
    }
    return result;
}

JsonMessage canToJsonSim(const CanMessageSimulator& simMsg) {
    // É praticamente o mesmo parse, só muda o tipo do objeto (nome da classe).
    int arbitrationId = simMsg.CAN_Message.ArbitrationId;
    const auto &data  = simMsg.CAN_Message.Data;

    std::string algorithmId = simMsg.AlgorithmID.empty()
                              ? "Unknown"
                              : simMsg.AlgorithmID;

    bool status = (data.size() > 0 && data[0] == 1);
    int distance = 0;
    if (data.size() > 2) {
        distance = (data[2] << 8) | data[1];
    }

    std::string side = "Esquerda";
    if (algorithmId == "BlindSpotDetection" && data.size() > 3 && data[3] == 1) {
        side = "Direita";
    }

    JsonMessage result;
    result.AlgorithmID = algorithmId;
    result.Timestamp   = "2025-01-31T12:00:00Z"; // Exemplo
    result.Status      = status;

    if (algorithmId == "BlindSpotDetection") {
        result.Data = {
            {"Side", side},
            {"DistanceToVehicle", distance / 100.0}
        };
    } else {
        result.Data = {
            {"DistanceToVehicle", distance / 100.0}
        };
    }

    return result;
}

/* -----------------------------------------------------------------------
   Callback para lidar com mensagens recebidas.
   -----------------------------------------------------------------------*/
class BrokerLogicCallback : public virtual mqtt::callback
{
public:
    // Recebe a referência do client, para podermos republicar mensagens.
    BrokerLogicCallback(mqtt::async_client& cli)
        : client_(cli)
    {
        // Inicializa dicionário de arbitration IDs para "sim/canmessages"
        simArbitrationMap_ = {
            { 0x100, "simsensor/blindspot" },
            { 0x101, "simsensor/pedestrian" },
            { 0x102, "simsensor/frontalcollision" },
            { 0x103, "simsensor/rearcollision" }
        };
    }

    // Método chamado quando chega uma mensagem
    void message_arrived(mqtt::const_message_ptr msg) override {
        std::string topic   = msg->get_topic();
        std::string payload = msg->to_string();

        std::cout << "\n[Recebido] Tópico: " << topic << "\n"
                  << "Payload: " << payload << std::endl;

        try {
            // Se o tópico começa com "sim/"
            if (topic.rfind("sim/", 0) == 0) {
                // Se for "sim/canmessages", parse especial
                if (topic == "sim/canmessages") {
                    // Tentar parse de JSON como CanMessageSimulator
                    auto j = json::parse(payload);
                    CanMessageSimulator simMsg;
                    simMsg.AlgorithmID = j.value("algorithm_id", "");
                    // Pegar can_message -> arbitration_id e data
                    if (j.contains("can_message")) {
                        auto cm = j["can_message"];
                        simMsg.CAN_Message.ArbitrationId = cm.value("arbitration_id", 0);
                        if (cm.contains("data") && cm["data"].is_array()) {
                            for (auto &d : cm["data"]) {
                                simMsg.CAN_Message.Data.push_back(d.get<int>());
                            }
                        }
                    }

                    // Log similar ao .NET
                    std::cout << "Arbitration ID: " << std::hex
                              << simMsg.CAN_Message.ArbitrationId << std::dec << "\n";
                    std::cout << "Data Bytes: ";
                    for (auto &b : simMsg.CAN_Message.Data) std::cout << b << " ";
                    std::cout << std::endl;

                    // Converter para JSON final
                    auto jsonMsg = canToJsonSim(simMsg);
                    json outPayload = {
                        {"AlgorithmID", jsonMsg.AlgorithmID},
                        {"Timestamp",   jsonMsg.Timestamp},
                        {"Status",      jsonMsg.Status},
                        {"Data",        jsonMsg.Data}
                    };

                    // Verificar se há um tópico mapeado no dictionary
                    int arb = simMsg.CAN_Message.ArbitrationId;
                    if (simArbitrationMap_.find(arb) != simArbitrationMap_.end()) {
                        std::string targetTopic = simArbitrationMap_[arb];

                        // Publica no tópico mapeado
                        publishMessage(targetTopic, outPayload.dump());
                        std::cout << "Mensagem redirecionada para " << targetTopic << std::endl;
                    } else {
                        std::cout << "ArbitrationId não mapeado para tópico específico." << std::endl;
                    }
                }
                else {
                    // Para outros sub-tópicos "sim/...", redirecionar para "moto/..."
                    std::string newTopic = "moto/" + topic.substr(4);
                    publishMessage(newTopic, payload);
                    std::cout << "(Simulação) Tópico: " << topic
                              << " -> Redirecionado para: " << newTopic
                              << " com valor: " << payload << std::endl;
                }
            }
            // Se o tópico for "can/messages"
            else if (topic == "can/messages") {
                auto j = json::parse(payload);
                CanMessage canMsg;
                canMsg.AlgorithmID = j.value("AlgorithmID", "");
                if (j.contains("CAN_Message")) {
                    auto cm = j["CAN_Message"];
                    canMsg.CAN_Message.ArbitrationId = cm.value("ArbitrationId", 0);
                    if (cm.contains("Data") && cm["Data"].is_array()) {
                        for (auto &d : cm["Data"]) {
                            canMsg.CAN_Message.Data.push_back(d.get<int>());
                        }
                    }
                }

                // Log
                std::cout << "Arbitration ID: " << std::hex
                          << canMsg.CAN_Message.ArbitrationId << std::dec << "\n";
                std::cout << "Data Bytes: ";
                for (auto &b : canMsg.CAN_Message.Data) std::cout << b << " ";
                std::cout << std::endl;

                // Converter para JSON final e publicar em "sensor/sensordetector"
                auto jsonMsg = canToJson(canMsg);
                json outPayload = {
                    {"AlgorithmID", jsonMsg.AlgorithmID},
                    {"Timestamp",   jsonMsg.Timestamp},
                    {"Status",      jsonMsg.Status},
                    {"Data",        jsonMsg.Data}
                };

                std::string targetTopic = "sensor/sensordetector";
                publishMessage(targetTopic, outPayload.dump());
                std::cout << "Mensagem redirecionada para o tópico " << targetTopic << std::endl;
            }
            // Se chegou aqui, não era "sim/" nem "can/messages"
            else {
                std::cout << "Tópico não previsto na lógica: " << topic << std::endl;
            }
        }
        catch (std::exception &ex) {
            std::cerr << "Erro ao processar mensagem: " << ex.what() << std::endl;
        }
    }

private:
    mqtt::async_client& client_;

    // Dicionário para mapear arbitrationId -> tópico, no caso "sim/canmessages"
    std::unordered_map<int, std::string> simArbitrationMap_;

    // Função auxiliar para publicar mensagens
    void publishMessage(const std::string &topic, const std::string &payload) {
        auto msg = mqtt::make_message(topic, payload);
        msg->set_qos(1);
        msg->set_retained(true); // se quiser replicar .WithRetainFlag()
        client_.publish(msg);
    }
};

/* -----------------------------------------------------------------------
   main(): Conecta ao broker Mosquitto, assina nos tópicos, e processa
   mensagens via callback.
   -----------------------------------------------------------------------*/
int main() {
    // Endereço do broker local (Mosquitto rodando em 1883)
    const std::string address   = "tcp://172.20.0.14:1883";
    const std::string clientId  = "CppBroker";

    std::cout << "Iniciando a lógica MQTT em C++..." << std::endl;

    // Cria cliente MQTT
    mqtt::async_client client(address, clientId);
    // Instancia callback com nossa lógica
    BrokerLogicCallback myCallback(client);
    client.set_callback(myCallback);

    // Opções de conexão
    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);

    try {
        std::cout << "Conectando ao broker " << address << "..." << std::endl;
        client.connect(connOpts)->wait();
        std::cout << "Conectado ao broker.\n";

        // Assina nos tópicos principais
        client.subscribe("sim/#", 1)->wait();
        client.subscribe("can/messages", 1)->wait();

        std::cout << "Assinatura concluída. Aguardando mensagens...\n";
        std::cout << "Pressione CTRL+C para encerrar.\n";

        // Mantém o programa vivo
        while(true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // (Opcional) se sair do loop, desconecta
        // client.disconnect()->wait();
    }
    catch(const mqtt::exception &ex) {
        std::cerr << "Erro na conexão MQTT: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
