# 消息队列mini-mq项目简介

## 概述
本项目是一个基于Java实现的消息队列系统。消息队列是一种常用的中间件技术，用于在分布式系统中解耦生产者与消费者，实现异步通信。通过本项目，您可以了解消息队列的基本工作原理，并在本地环境中进行测试和使用。

## 功能特性
1. **消息发布与订阅**：支持多个生产者和消费者，生产者可以将消息发布到队列中，消费者可以订阅并消费消息。
2. **消息持久化**：消息在发布时会被持久化存储，确保在系统重启后消息不会丢失。
3. **消息确认机制**：消费者在成功处理消息后，可以向队列发送确认信号，确保消息被正确处理。
4. **多队列支持**：支持创建多个消息队列，每个队列可以独立管理消息。
5. **简单易用**：提供简洁的API接口，方便开发者快速集成和使用。

## 快速开始

### 环境要求
- JDK 1.8 或更高版本
- Maven 3.x





完善中......

