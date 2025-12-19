/* tslint:disable */
/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { GoogleGenAI, Modality } from '@google/genai';
import { LitElement, css, html } from 'lit';

/* =======================
   SHIM process (browser)
======================= */
if (typeof window !== 'undefined') {
  window.process = window.process || {};
  window.process.env = window.process.env || {};
}

/* =======================
   UTILS
======================= */
function encode(bytes) {
  let binary = '';
  const len = bytes.byteLength;
  for (let i = 0; i < len; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
}

function decode(base64) {
  const binaryString = atob(base64);
  const len = binaryString.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = binaryString.charCodeAt(i);
  return bytes;
}

function downsample(buffer, inRate, outRate) {
  if (inRate === outRate) return buffer;
  const ratio = inRate / outRate;
  const newLength = Math.round(buffer.length / ratio);
  const result = new Float32Array(newLength);
  let offset = 0;
  for (let i = 0; i < newLength; i++) {
    result[i] = buffer[Math.floor(offset)];
    offset += ratio;
  }
  return result;
}

function createBlob(data) {
  const l = data.length;
  const int16 = new Int16Array(l);
  for (let i = 0; i < l; i++) int16[i] = Math.max(-1, Math.min(1, data[i])) * 32768;
  return {
    data: encode(new Uint8Array(int16.buffer)),
    mimeType: 'audio/pcm;rate=16000',
  };
}

async function decodeAudioData(data, ctx, sampleRate, numChannels) {
  const buffer = ctx.createBuffer(
    numChannels,
    data.length / 2 / numChannels,
    sampleRate
  );

  const dataInt16 = new Int16Array(data.buffer);
  const l = dataInt16.length;
  const dataFloat32 = new Float32Array(l);
  for (let i = 0; i < l; i++) dataFloat32[i] = dataInt16[i] / 32768.0;

  buffer.copyToChannel(dataFloat32, 0);
  return buffer;
}

/* =======================
   COMPONENT
======================= */
class GdmLiveAudio extends LitElement {
  static properties = {
    isActive: { type: Boolean, state: true },
    isReady: { type: Boolean, state: true },
  };

  static styles = css`
    :host {
      position: fixed;
      bottom: 2rem;
      right: 2rem;
      z-index: 9999;
    }
    button {
      width: 70px;
      height: 70px;
      border-radius: 50%;
      border: none;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      box-shadow: 0 4px 20px rgba(102, 126, 234, 0.5);
      transition: all 0.3s ease;
    }
    button:disabled {
      background: #444;
      cursor: not-allowed;
      opacity: 0.6;
    }
    button.active {
      background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
      animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
      0%, 100% { box-shadow: 0 0 0 0 rgba(231, 76, 60, 0.7); }
      50% { box-shadow: 0 0 0 20px rgba(231, 76, 60, 0); }
    }
    button svg {
      width: 36px;
      height: 36px;
      fill: white;
    }
  `;

  constructor() {
    super();
    this.isActive = false;
    this.isReady = false;

    /* SINGLE AudioContext */
    this.audioContext = new (window.AudioContext || window.webkitAudioContext)();

    this.inputNode = this.audioContext.createGain();
    this.outputNode = this.audioContext.createGain();
    this.outputNode.connect(this.audioContext.destination);

    this.mediaStream = null;
    this.sourceNode = null;
    this.scriptProcessorNode = null;

    this.nextStartTime = 0;

    this.initClient();
  }

  async initClient() {
    this.client = new GoogleGenAI({
      apiKey: 'AIzaSyAbK8Cs1I_XNebSr-04hrygdQNjvew4BUc',
    });

    await this.initSession();
  }

  async initSession() {
    this.session = await this.client.live.connect({
      model: 'gemini-2.0-flash-exp',
      callbacks: {
        onopen: () => { this.isReady = true; },
        onmessage: async (message) => {
          const audio = message.serverContent?.modelTurn?.parts[0]?.inlineData;
          if (!audio) return;

          this.nextStartTime = Math.max(
            this.nextStartTime,
            this.audioContext.currentTime
          );

          const audioBuffer = await decodeAudioData(
            decode(audio.data),
            this.audioContext,
            24000,
            1
          );

          const source = this.audioContext.createBufferSource();
          source.buffer = audioBuffer;
          source.connect(this.outputNode);
          source.start(this.nextStartTime);
          this.nextStartTime += audioBuffer.duration;
        },
        onerror: () => { },
        onclose: () => { this.isReady = false; },
      },
      config: {
        responseModalities: [Modality.AUDIO],
        systemInstruction: {
          parts: [{ text: 'Você é o assistente SNIIC. Responda em Português.' }],
        },
        speechConfig: {
          voiceConfig: { prebuiltVoiceConfig: { voiceName: 'Aoede' } },
          languageCode: 'pt-BR',
        },
      },
    });
  }

  async startConversation() {
    if (this.audioContext.state === 'suspended') {
      await this.audioContext.resume();
    }

    this.mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });

    this.sourceNode =
      this.audioContext.createMediaStreamSource(this.mediaStream);

    this.scriptProcessorNode =
      this.audioContext.createScriptProcessor(2048, 1, 1);

    this.scriptProcessorNode.onaudioprocess = (e) => {
      if (!this.isActive || !this.session) return;

      const pcm = e.inputBuffer.getChannelData(0);
      const resampled = downsample(
        pcm,
        this.audioContext.sampleRate,
        16000
      );

      this.session.sendRealtimeInput({
        media: createBlob(resampled),
      });
    };

    this.sourceNode.connect(this.scriptProcessorNode);
    this.scriptProcessorNode.connect(this.audioContext.destination);

    this.isActive = true;
  }

  stopConversation() {
    this.isActive = false;
    if (this.mediaStream) {
      this.mediaStream.getTracks().forEach((t) => t.stop());
    }
    if (this.scriptProcessorNode) this.scriptProcessorNode.disconnect();
    if (this.sourceNode) this.sourceNode.disconnect();
  }

  async handleClick() {
    if (!this.isReady) return;
    this.isActive ? this.stopConversation() : await this.startConversation();
  }

  render() {
    return html`
      <button
        ?disabled=${!this.isReady}
        class="${this.isActive ? 'active' : ''}"
        @click=${this.handleClick}
      >
        <svg viewBox="0 0 24 24">
          <path
            d="M12 14c1.66 0 3-1.34 3-3V5c0-1.66-1.34-3-3-3S9 3.34 9 5v6c0 1.66 1.34 3 3 3z"
          />
          <path
            d="M17 11c0 2.76-2.24 5-5 5s-5-2.24-5-5H5c0 3.53 2.61 6.43 6 6.92V21h2v-3.08c3.39-.49 6-3.39 6-6.92h-2z"
          />
        </svg>
      </button>
    `;
  }
}

customElements.define('gdm-live-audio', GdmLiveAudio);
