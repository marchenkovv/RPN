import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True, slots=True)
class PatientRecord:
    """Запись пациента для сверки."""
    enp: str
    bp: str
    fam: str
    im: str
    ot: str
    dr: str

    @property
    def full_key(self) -> Tuple[str, ...]:
        return self.enp, self.bp, self.fam, self.im, self.ot, self.dr

    @property
    def short_key(self) -> Tuple[str, str]:
        return self.enp, self.bp

    @classmethod
    def from_xml(cls, zap: ET.Element) -> 'PatientRecord':
        return cls(
            enp=zap.findtext('ENP', ''),
            bp=zap.findtext('BP', ''),
            fam=zap.findtext('FAM', ''),
            im=zap.findtext('IM', ''),
            ot=zap.findtext('OT', ''),
            dr=zap.findtext('DR', ''),
        )

    @property
    def is_valid(self) -> bool:
        return bool(self.enp and self.bp)
